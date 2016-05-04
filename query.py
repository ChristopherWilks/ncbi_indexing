#!/usr/bin/env python2.7
import sys
import re

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
#HEADER = [['journal_t',TextField],['title_s',StringField],['authors_t',TextField],['author_info_t',TextField],['abstract_t',TextField],['pmid_s',StringField]]
pubmed_field_set = set()
[pubmed_field_set.add(x[0]) for x in parse_abstracts.HEADER]

sra_field_set = set(["all","raw","SAMPLE_alias","SAMPLE_DESCRIPTION","SUBMISSION_TITLE","EXPERIMENT_INSTRUMENT_MODEL","STUDY_alias","STUDY_STUDY_ABSTRACT","SAMPLE_accession","EXPERIMENT_LIBRARY_STRATEGY","EXPERIMENT_alias","EXPERIMENT_TITLE","EXPERIMENT_LIBRARY_NAME","EXPERIMENT_DESIGN_DESCRIPTION","EXPERIMENT_LIBRARY_SELECTION","SAMPLE_TITLE","STUDY_accession","EXPERIMENT_LIBRARY_SOURCE","EXPERIMENT_LIBRARY_CONSTRUCTION_PROTOCOL","STUDY_STUDY_TITLE","EXPERIMENT_accession"])

lucene.initVM()
analyzer = StandardAnalyzer(Version.LUCENE_4_10_1)
analyzer2 = WhitespaceAnalyzer(Version.LUCENE_4_10_1)

pubmed = IndexReader.open(SimpleFSDirectory(File("./lucene_pubmed_index")))
sra = IndexReader.open(SimpleFSDirectory(File("./lucene_sra_index")))
psearcher = IndexSearcher(pubmed)
ssearcher = IndexSearcher(sra)

NUM_TO_RETRIEVE = 100

def search_lucene(fields_,terms_,requirements_,searcher,index=0):
  terms = []
  fields = []
  requirements = []
  for (i,x) in enumerate(terms_):
    terms.append(x[index])
    fields.append(fields_[i][index])
    requirements.append(requirements_[i][index])
  sys.stderr.write("Running query index %d: %s in %s restricted to %s\n" % (index,",".join(terms),",".join(fields),",".join([str(x) for x in requirements])))
  query = MultiFieldQueryParser.parse(Version.LUCENE_4_10_1,terms,fields,requirements,analyzer2)
  return(searcher.search(query, NUM_TO_RETRIEVE))

#sample queries:
#1) human lung cancer => field=raw,value = human lung cancer
#2) human AND lung AND cancer => field=raw,value = human AND lung AND cancer
#3) EXPERIMENT_TITLE::lung cancer;;SAMPLE_SAMPLE_ABSTRACT::adenocarncinoma => fields=[EXPERIMENT_TITLE],SAMPLE_SAMPLE_ABSTRACT],values=[lung cancer,adenocarncinoma]
#3b) all::tag1:value1;;all::tag2:value2 => fields=[all,all],values=[tag1:value1,tag2:value2]
#returns fields and values to search on for both pubmed (index 0) and sra (index 1)
field_patt = re.compile(r'::')
FIELD_DELIM=';;'
FIELD_VAL_DELIM='::'
def parse_query(query):
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
  return (fields,values,requirements) 

def process_query(query):
  (fields,values,requirements) = parse_query(query)
  presults = search_lucene(fields,values,requirements,psearcher,index=0) 
  sresults = search_lucene(fields,values,requirements,ssearcher,index=1) 
  sys.stderr.write("results: %d in pubmed; %d in sra\n" % (presults.totalHits,sresults.totalHits))

def main():
  if len(sys.argv) < 2:
    sys.stderr.write("need query\n")
    sys.exit(-1)
  query = sys.argv[1]
  process_query(query)

if __name__ == '__main__':
  main() 
