#!/bin/env python2.7
import sys
import re 
import lxml.etree as xparser
import gzip
import glob

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField

from lucene_indexer import LuceneIndexer

fields_seen = set(['all','raw'])

#attributes to parse:
identifiers = {'PRIMARY_ID':1,'SUBMITTER_ID':1,'EXTERNAL_ID':1}
elements = {'EXPERIMENT':{'attrs':set(['alias','accession']),'TITLE':1,'DESIGN_DESCRIPTION':1,'LIBRARY_DESCRIPTOR':2,'LIBRARY_CONSTRUCTION_PROTOCOL':1,'INSTRUMENT_MODEL':1,'EXPERIMENT_ATTRIBUTES':2},'SUBMISSION':{'attrs':set([]),'TITLE':1},'STUDY':{'attrs':set(['alias','accession']),'STUDY_TITLE':1,'STUDY_ABSTRACT':1},'SAMPLE':{'attrs':set(['alias','accession']),'TITLE':1,'DESCRIPTION':1,'SAMPLE_ATTRIBUTES':2}}

def print_doc(fields,header,accession):
  print("Accession %s:" % (accession))
  for (idx,field) in enumerate(fields):
    print("\t%s:%s:%s" % (field,header[idx][0],header[idx][1]))

#handle 2 cases:
#1) list of *_ATTRIBUTEs
#2) one-level-down elements (no DFS here)
attributes_patt = re.compile('_ATTRIBUTES$')
def process_sub_children(sub_e,fields,header,top_e):
  attributes = False
  if attributes_patt.search(sub_e.tag):
    sub_e = sub_e.findall('.//%s' % sub_e.tag[0:-1])
    attributes = True
  for child in sub_e:
    if attributes:
      if len(child) > 1 and child[0] != None and child[1] != None:
        #TAG
        #header.append([child[0].text,TextField])
        #VALUE
        #print("%s %s\n" % (child.tag,child[1].tag))
        #fields.append(child[1].text) 
        #actaully only add these to the first field which is all the attributes concatenated by spaces
        fields[0].append("%s:%s" % (child[0].text,child[1].text))
    elif child.text != None:
      fieldname = "%s_%s" % (top_e,child.tag)
      header.append([fieldname,TextField])
      if fieldname not in fields_seen: 
        fields_seen.add(fieldname)
      fields.append(child.text)
      fields[0].append(child.text)

#TODO: parse RUNs (especially SRRs)
#parse one EXPERIMENT_PACKAGE section (one document in lucene)
def parse_exp(exp_xml,li):
  fields = [[]]
  header = [['all',TextField]]
  accessions = set()
  accession = 'NA'
  for (top_e,subs) in elements.iteritems():
    e = exp_xml.find(top_e)
    if e == None:
      continue
    for (k,v) in subs.iteritems():
      if k == 'attrs':
        for attr_ in v:
          attr = e.get(attr_)
          if attr_ == 'accession':
            accessions.add(attr)
            if top_e == 'EXPERIMENT':
              accession = attr 
          if attr != None:
            fields.append(attr)
            fieldname = "%s_%s" % (top_e,attr_)
            header.append([fieldname,StringField])
            if fieldname not in fields_seen: 
              fields_seen.add(fieldname)
            fields[0].append(attr)
      else:
          sube = e.find(".//%s" % k)
          if sube == None:
            continue
          if v == 2:
            process_sub_children(sube,fields,header,top_e)
          elif sube.text != None:
            fields.append(sube.text)
            fieldname = "%s_%s" % (top_e,sube.tag)
            header.append([fieldname,TextField])
            if fieldname not in fields_seen: 
              fields_seen.add(fieldname)
            fields[0].append(sube.text)

  fields.append(xparser.tostring(exp_xml))
  header.append(['raw',TextField])
  if len(fields) > 0:
    fields[0] = ' '.join(fields[0])
  #print_doc(fields,header,accession)
  print("adding_document\t%s\t%s" % (accession,";".join(sorted(accessions))))
  li.add_document(fields,header,accession)
          
def parse_sra_chunk(xml,li):
  root = xparser.fromstring(xml)
  exps = root.findall('EXPERIMENT_PACKAGE')
  for exp in exps:
    parse_exp(exp,li) 

if __name__ == '__main__':
  li = LuceneIndexer("./lucene_sra_index")
  files_ = glob.glob('*.gz')
  for f in files_:
    #f = sys.argv[1]
    with gzip.open(f,"r") as fin:
      xml = fin.read()
      parse_sra_chunk(xml,li)
  li.close()
  for fieldname in fields_seen:
    sys.stdout.write("%s\n" % (fieldname)) 