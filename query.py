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
    fields = ['raw','raw']
    values = [terms[0],terms[0]]
    requirements = [BooleanClause.Occur.MUST,BooleanClause.Occur.MUST]
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
  (genes,accessions) = ie.extract_identifiers("NA",0,raw_text)
  return (fields,values,requirements,genes,accessions) 

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

def process_query(ie,genes2ids,query):
  #get the query parsed into its fields, their values, and the boolean requirements (MUST or SHOULD)
  #also extract out any gene names and/or SRA accessions
  (fields,values,requirements,genes,accessions) = parse_query(ie,query)

  (pterms,pfields,preqs,presults) = search_lucene(fields,values,requirements,psearcher,index=0) 
  (sterms,sfields,sreqs,sresults) = search_lucene(fields,values,requirements,ssearcher,index=1) 
  sys.stdout.write("results: %d in pubmed; %d in sra\n" % (presults.totalHits,sresults.totalHits))

  #find the ids of both (either/or) pubmed and SRA entries which contains references to one or more
  #of the genenames in the query 
  (pubmed_ids,sra_ids) = get_ids_by_genenames(genes2ids,genes)
  sra_ids.update(accessions)
 
  parse_results('PMID',pterms,pfields,preqs,presults,psearcher,id_filter=pubmed_ids)
  parse_results('EXPERIMENT_accession',sterms,sfields,sreqs,sresults,ssearcher,id_filter=sra_ids)


ID_BOOST=1000
def relevance_sort(scoreDocs,primary_id_field,searcher,id_filter):
  #do something to incorporate id_filter
  for d in scoreDocs:
    pid = searcher.doc(d.doc).get(primary_id_field)
    if pid in id_filter:
      d.score+=ID_BOOST
  return sorted(scoreDocs,key=lambda x: x.score,reverse=True)

def parse_results(primary_id_field,terms,fields,reqs,results,searcher,id_filter=set()):
  sys.stdout.write("filter set %d\n" % (len(id_filter)))
  for r in relevance_sort(results.scoreDocs,primary_id_field,searcher,id_filter):
    docu = searcher.doc(r.doc)
    pid = docu.get(primary_id_field)
    have_gene = False
    if pid in id_filter:
      have_gene = True
    sys.stdout.write("%s: %s %d %s\n" % (primary_id_field,pid,r.score,have_gene))
    for f in fields:
      f_ = docu.get(f)
      #sys.stderr.write("%s\t%s\n" % (f,f_))

#pubmed,sra
PUBMED_IDX=0
SRA_IDX=1

ID_COL=[1,1]
GENE_COL=[2,3]
def load_gene2id_map(files):
  genes2ids = {}
  #for faster loading (serialized binary) look for pickled file
  #UPDAE: not faster, pickling/loading from takes ~4-5 seconds longer
  #pkl_file = "genes2ids_map.pkl"
  #if os.path.exists(pkl_file):
  #  with open(pkl_file,"rb") as fin_:
  #    genes2ids = pickle.load(fin_)
  #    return genes2ids
  for (idx,file_) in enumerate(files):
    with open(file_,"r") as fin:
      for line in fin:
        fields = line.rstrip('\n').split("\t")
        #print("%d %s" % (idx,line))
        id_ = fields[ID_COL[idx]]
        if idx == SRA_IDX and len(id_) > 3:
          #avoid the redundancy of storing the full "SRX" prefix
          id_ = id_[3:]
        genes = fields[GENE_COL[idx]].split(";")
        for gene in genes:
          if gene not in genes2ids:
            #pubmed,sra
            genes2ids[gene] = [set(),set()]
          genes2ids[gene][idx].add(int(id_))
  #with open(pkl_file,"wb") as fout_:
  #  pickle.dump(genes2ids, fout_)
  return genes2ids


def main():
  if len(sys.argv) < 2:
    sys.stderr.write("need query\n")
    sys.exit(-1)
  ie = IdentifierExtracter(hugo_genenamesF,gene_filter=re.compile(r'[\-\d]'),filter_stopwords=True)
  genes2ids = load_gene2id_map(["pubmed_map.tsv","sra_map.tsv"])
  query = sys.argv[1]
  process_query(ie,genes2ids,query)

if __name__ == '__main__':
  main() 
