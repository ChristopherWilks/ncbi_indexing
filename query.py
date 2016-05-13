#!/usr/bin/env python2.7
import sys
import os
import re

import pickle

#based on the example code at
#http://graus.nu/blog/pylucene-4-0-in-60-seconds-tutorial/
import lucene
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.document import Document, Field
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search import BooleanQuery
from org.apache.lucene.search import TermQuery
from org.apache.lucene.search import NumericRangeQuery
from org.apache.lucene.index import IndexReader
from org.apache.lucene.index import Term
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.search import BooleanClause
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

import parse_abstracts
from IdentityExtractor import IdentifierExtracter


pubmed_field_set = set()
[pubmed_field_set.add(x[0]) for x in parse_abstracts.HEADER]

sym2name={0:'PubMed',1:'SRA','+':'BooleanClause.Occur.MUST'}

sra_field_set = set(["all","raw","SAMPLE_alias","SAMPLE_DESCRIPTION","SUBMISSION_TITLE","EXPERIMENT_INSTRUMENT_MODEL","STUDY_alias","STUDY_STUDY_ABSTRACT","SAMPLE_accession","EXPERIMENT_LIBRARY_STRATEGY","EXPERIMENT_alias","EXPERIMENT_TITLE","EXPERIMENT_LIBRARY_NAME","EXPERIMENT_DESIGN_DESCRIPTION","EXPERIMENT_LIBRARY_SELECTION","SAMPLE_TITLE","STUDY_accession","EXPERIMENT_LIBRARY_SOURCE","EXPERIMENT_LIBRARY_CONSTRUCTION_PROTOCOL","STUDY_STUDY_TITLE","EXPERIMENT_accession"])

lucene.initVM()
analyzer = StandardAnalyzer(Version.LUCENE_4_10_1)
analyzer2 = WhitespaceAnalyzer(Version.LUCENE_4_10_1)

pubmed = IndexReader.open(SimpleFSDirectory(File("./lucene_pubmed_index")))
sra = IndexReader.open(SimpleFSDirectory(File("./lucene_sra_index")))
psearcher = IndexSearcher(pubmed)
ssearcher = IndexSearcher(sra)

NUM_TO_RETRIEVE = 100
hugo_genenamesF = 'refFlat.hg38.txt.sorted'

def search_lucene(fields_,terms_,requirements_,searcher,index=0):
  terms = []
  fields = []
  requirements = []
  for (i,x) in enumerate(terms_):
    terms.append(x[index])
    fields.append(fields_[i][index])
    requirements.append(requirements_[i][index])
  sys.stderr.write("Running query %s: (\"%s\") in fields (%s) with requirements (%s)\n" % (sym2name[index],"\",\"".join(terms),",".join(fields),",".join([sym2name[str(x)] for x in requirements])))
  query = MultiFieldQueryParser.parse(Version.LUCENE_4_10_1,terms,fields,requirements,analyzer2)
  return(terms,fields,requirements,searcher.search(query, NUM_TO_RETRIEVE))

#sample queries:
#1) human lung cancer => field=raw,value = human lung cancer
#2) human AND lung AND cancer => field=raw,value = human AND lung AND cancer
#3) EXPERIMENT_TITLE::lung cancer;;SAMPLE_SAMPLE_ABSTRACT::adenocarncinoma => fields=[EXPERIMENT_TITLE],SAMPLE_SAMPLE_ABSTRACT],values=[lung cancer,adenocarncinoma]
#3b) all::tag1:value1;;all::tag2:value2 => fields=[all,all],values=[tag1:value1,tag2:value2]
#returns fields and values to search on for both pubmed (index 0) and sra (index 1)
field_patt = re.compile(r'::')
FIELD_DELIM=';;'
FIELD_VAL_DELIM='::'
def parse_query(ie,query):
  terms = query.split(FIELD_DELIM)
  fields = []
  values = []
  requirements = []
  if len(terms) == 1 and field_patt.search(terms[0]) is None:
    fields.append(['raw','raw'])
    values.append([terms[0],terms[0]])
    requirements.append([BooleanClause.Occur.MUST,BooleanClause.Occur.MUST])
  else:
    for term in terms:
      (field,val) = term.split(FIELD_VAL_DELIM)
      #going to be the same for both searchers usually
      vals = [val,val]
      requirements.append([BooleanClause.Occur.MUST,BooleanClause.Occur.MUST])
      if field in pubmed_field_set:
        fields.append([field,'raw'])
      elif field in sra_field_set:
        fields.append(['raw',field])
        if field == 'all':
          #this is special to the SRA case, so we'll split by tag:value delimiter ":"
          #and then add them into the pubmed one with a space (OR) in between
          (val1,val2) = val.split(':')
          vals = ["%s %s" % (val1,val2),val]
      #if we don't know what the field is
      else:
        fields.append(['raw','raw']) 
      values.append(vals)
  raw_text = ' '.join(set([z for k in values for z in k]))
  (genes,accessions,pmids) = ie.extract_identifiers("NA",0,raw_text)
  return (fields,values,requirements,genes,accessions,pmids) 

def get_ids_by_genenames(genes2ids,genes):
  pm_ids = set()
  sra_ids = set()
  for gene in genes:
    try:
      (pids,sids) = genes2ids[gene]
      pm_ids.update(pids) 
      sra_ids.update(sids) 
    except KeyError, ke:
      continue 
  #map SRX ids back to their full id for matching
  sra_ids = set(map(lambda z: "SRX%s" % z,sra_ids))
  return (pm_ids,sra_ids)

def process_query(ie,genes2ids,id2additional_ids,query):
  #get the query parsed into its fields, their values, and the boolean requirements (MUST or SHOULD)
  #also extract out any gene names and/or SRA accessions and/or PMIDs, though we're only interested in genenames at this time
  (fields,values,requirements,genes,accessions,pmids) = parse_query(ie,query)

  (pterms,pfields,preqs,presults) = search_lucene(fields,values,requirements,psearcher,index=0) 
  (sterms,sfields,sreqs,sresults) = search_lucene(fields,values,requirements,ssearcher,index=1) 
  sys.stdout.write("results: %d in pubmed; %d in sra\n" % (presults.totalHits,sresults.totalHits))

  #find the ids of both (either/or) pubmed and SRA entries which contains references to one or more
  #of the genenames in the query 
  (pubmed_ids,sra_ids) = get_ids_by_genenames(genes2ids,genes)
  sra_ids.update(accessions)
 
  parse_results(['PMID','EXPERIMENT_accession'],[presults,sresults],[psearcher,ssearcher],id2additional_ids,id_filters=[pubmed_ids,sra_ids])


#def relevance_sort(scoreDocs,primary_id_field,searcher,id_filter):

ID_BOOST=1000
def score_results_for_id(result,searcher,primary_id_field,id_filter,final_results,idx):
  for scoreDoc in result.scoreDocs:
    pid = searcher.doc(scoreDoc.doc).get(primary_id_field)
    if pid in id_filter:
      scoreDoc.score+=ID_BOOST
    #add both the scoreDoc AND the id (pubmed or sra)
    final_results.append([idx,scoreDoc])

def parse_results(primary_id_fields,results,searchers,id2additional_ids,id_filters=[set(),set()]):
  sys.stdout.write("filter set: %d %d\n" % (len(id_filters[0]),len(id_filters[1])))
  merged_results = []
  for (idx, result) in enumerate(results):
    score_results_for_id(result,searchers[idx],primary_id_fields[idx],id_filters[idx],merged_results,idx)
  for sdoc in sorted(merged_results,key=lambda x: x[1].score,reverse=True):
    idx = sdoc[0]
    scoreDoc = sdoc[1]
    pfield = primary_id_fields[idx]
    pid = searchers[idx].doc(scoreDoc.doc).get(pfield)
    have_gene = False
    #if scoreDoc.score >= 1000:
    #  have_gene = True
    additional_ids = ""
    if pid in id2additional_ids:
      additional_ids = id2additional_ids[pid]
    sys.stdout.write("%s: %s %d %s\n" % (pfield,pid,scoreDoc.score,additional_ids))
    #for f in fields:
    #  f_ = docu.get(f)
      #sys.stderr.write("%s\t%s\n" % (f,f_))

#pubmed,sra
PUBMED_IDX=0
SRA_IDX=1

ID_COL=[1,1]
GENE_COL=[2,3]
ADD_IDS_START_COL=[2,2]
def load_gene2id_map(files,print_additional_ids=False):
  genes2ids = {}
  id2additional_ids = {}
  #for faster loading (serialized binary) look for pickled file
  #UPDATE: not faster, pickling/loading from takes ~4-5 seconds longer
  for (idx,file_) in enumerate(files):
    with open(file_,"r") as fin:
      for line in fin:
        fields = line.rstrip('\n').split("\t")
        #print("%d %s" % (idx,line))
        id_ = fields[ID_COL[idx]]
        if print_additional_ids and len(fields[ADD_IDS_START_COL[idx]]) > 0:
          id2additional_ids[str(id_)]="\t".join(fields[ADD_IDS_START_COL[idx]:])
        if idx == SRA_IDX and len(id_) > 3:
          #avoid the redundancy of storing the full "SRX" prefix
          id_ = id_[3:]
        genes = fields[GENE_COL[idx]].split(";")
        for gene in genes:
          if gene not in genes2ids:
            #pubmed,sra
            genes2ids[gene] = [set(),set()]
          genes2ids[gene][idx].add(int(id_))
  return (genes2ids,id2additional_ids)

def main():
  if len(sys.argv) < 2:
    sys.stderr.write("need query\n")
    sys.exit(-1)
  query = sys.argv[1]
  print_additional_ids = False
  if len(sys.argv) >= 3:
    print_additional_ids = True;
  ie = IdentifierExtracter(hugo_genenamesF,gene_filter=re.compile(r'[\-\d]'),filter_stopwords=True)
  (genes2ids,id2additional_ids) = load_gene2id_map(["pubmed_map.tsv","sra_map.tsv"],print_additional_ids=print_additional_ids)
  process_query(ie,genes2ids,id2additional_ids,query)

if __name__ == '__main__':
  main() 
