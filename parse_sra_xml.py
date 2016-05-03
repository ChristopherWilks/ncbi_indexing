#!/bin/env python2.7
import sys
import re 
import lxml.etree as xparser

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField

from lucene_indexer import LuceneIndexer

#attributes to parse:
identifiers = {'PRIMARY_ID':1,'SUBMITTER_ID':1,'EXTERNAL_ID':1}
elements = {'EXPERIMENT':{'attrs':set(['alias','accession']),'TITLE':1,'DESIGN_DESCRIPTION':1,'LIBRARY_DESCRIPTOR':2,'LIBRARY_CONSTRUCTION_PROTOCOL':1,'INSTRUMENT_MODEL':1,'EXPERIMENT_ATTRIBUTES':2},'SUBMISSION':{'attrs':set([]),'TITLE':1},'STUDY':{'attrs':set(['alias','accession']),'STUDY_TITLE':1,'STUDY_ABSTRACT':1},'SAMPLE':{'attrs':set(['alias','accession']),'TITLE':1,'DESCRIPTION':1,'SAMPLE_ATTRIBUTES':2}}

def print_doc(fields,header,accession):
  print("Accession:")
  for (idx,field) in enumerate(fields):
    print("\t%s:%s:%s" % (field,header[idx][0],header[idx][1]))

attributes_patt = re.compile('_ATTRIBUTES$')
#parse one EXPERIMENT_PACKAGE section (one document in lucene)
def parse_exp(exp_xml):
  fields = []
  header = []
  accession = 'NA'
  for (top_e,subs) in elements.iteritems():
    e = exp_xml.find(top_e)
    if e == None:
      continue
    for (k,v) in subs.iteritems():
      if k == 'attrs':
        for attr in v:
          fields.append(e.get(attr))
          header.append(["%s_%s" % (top_e,attr),StringField])
      else:
          sube = e.find(".//%s" % k)
          if sube == None:
            continue
          #TODO fix attributes processing
          if attributes_patt.search(k):
            continue
          fields.append(sube.text)
          header.append(["%s_%s" % (top_e,k),TextField])
  print_doc(fields,header,accession)
  #li.add_document(fields,header,accession)
          
def parse_sra_chunk(xml):
  root = xparser.fromstring(xml)
  exps = root.findall('EXPERIMENT_PACKAGE')
  for exp in exps:
    parse_exp(exp) 

if __name__ == '__main__':
  f = sys.argv[1]
  #li = LuceneIndexer("./lucene_sra_index")
  with open(f,"r") as fin:
    xml = fin.read()
    parse_sra_chunk(xml) 
