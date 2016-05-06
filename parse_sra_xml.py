#!/bin/env python2.7
import sys
import re 
import lxml.etree as xparser
import gzip
import glob
from xml import sax

#expanded from http://stackoverflow.com/questions/17530471/python-get-all-text-from-an-xml-document
#only used for the raw exraction of all text, space delimited for word searching
class FullXML2TextHandler(sax.handler.ContentHandler):
    def parseString(self, string):
        self.text = []
        sax.parseString(string, self)
        return ' '.join(self.text)

    def characters(self, data):
        self.text.append(data)
    
    def startElement(self, name, attrs):
        for name in attrs.getNames():
          self.text.append(attrs.getValue(name))

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField

from lucene_indexer import LuceneIndexer

from IdentityExtractor import IdentifierExtracter

fields_seen = set(['all','raw'])

#attributes to parse:
identifiers = {'PRIMARY_ID':1,'SUBMITTER_ID':1,'EXTERNAL_ID':1}
elements = {'EXPERIMENT':{'attrs':set(['alias','accession']),'TITLE':1,'DESIGN_DESCRIPTION':1,'LIBRARY_DESCRIPTOR':2,'LIBRARY_CONSTRUCTION_PROTOCOL':1,'INSTRUMENT_MODEL':1,'EXPERIMENT_ATTRIBUTES':2},'SUBMISSION':{'attrs':set([]),'TITLE':1},'STUDY':{'attrs':set(['alias','accession']),'STUDY_TITLE':1,'STUDY_ABSTRACT':1},'SAMPLE':{'attrs':set(['alias','accession']),'TITLE':1,'DESCRIPTION':1,'SAMPLE_ATTRIBUTES':2}}

#pubmed ids are in <XREF_LINK><DB>pubmed</DB><ID>25499081</ID></XREF_LINK>

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

def add_element_attributes(element,attribute_names,fields,accessions):
  for attribute_name in attribute_names:
    value = element.get(attribute_name)
    if value != None:
      fields[0].append("%s:%s" % (attribute_name,value))
      if attribute_name == 'accession':
        accessions.add(value)

#mainly to get the RUN accession and RUN_ATTRIBUTES
def parse_run_set(exp_xml,header,fields,accessions):
  runs = exp_xml.findall(".//RUN")
  for run in runs:
    add_element_attributes(run,['alias','accession'],fields,accessions)
    rattrs = run.find(".//RUN_ATTRIBUTES")
    #just add the attributes to the list of ":" separated tag:value fields
    if rattrs != None:
      process_sub_children(rattrs,fields,header,run.text)

def parse_raw_text_for_genes(exp_xml,ie):
  raw_text = FullXML2TextHandler().parseString(xparser.tostring(exp_xml,pretty_print=True))
  (genes,accessions) = ie.extract_identifiers("NA",0,raw_text)
  return genes

#parse one EXPERIMENT_PACKAGE section (one document in lucene)
def parse_exp(exp_xml,li,ie):
  fields = [[]]
  header = [['all',TextField]]
  accessions = set()
  accession = 'NA'
  found_genes = parse_raw_text_for_genes(exp_xml,ie)
  for (top_e,subs) in elements.iteritems():
    e = exp_xml.find(top_e)
    #make sure to grab all pubmed id links
    if top_e == 'STUDY':
      xlinks = e.findall(".//XREF_LINK")
      for xlink in xlinks:
        if len(xlink) > 1 and xlink[0].text == 'pubmed':
          fields[0].append("%s:%s" % (xlink[0].text,xlink[1].text))
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

  parse_run_set(exp_xml,header,fields,accessions)
  fields.append(";".join(sorted(found_genes)))
  header.append(['genes',TextField])
  fields.append(xparser.tostring(exp_xml))
  header.append(['raw',TextField])
  if len(fields) > 0:
    fields[0] = ' '.join(fields[0])
  #print_doc(fields,header,accession)
  print("adding_document\t%s\t%s\t%s" % (accession,";".join(sorted(accessions)),";".join(sorted(found_genes))))
  li.add_document(fields,header,accession)
          
def parse_sra_chunk(xml,li,ie):
  root = xparser.fromstring(xml)
  exps = root.findall('EXPERIMENT_PACKAGE')
  for exp in exps:
    parse_exp(exp,li,ie) 

hugo_genenamesF = 'refFlat.hg38.txt.sorted'
def main():
  
  li = LuceneIndexer("./lucene_sra_index2")
  ie = IdentifierExtracter(hugo_genenamesF,gene_filter=re.compile(r'[\-\d]'),filter_stopwords=True)
  files_ = glob.glob('*.gz')
  for f in files_:
    with gzip.open(f,"r") as fin:
      xml = fin.read()
      parse_sra_chunk(xml,li,ie)
  li.close()
  for fieldname in fields_seen:
    sys.stdout.write("%s\n" % (fieldname)) 

if __name__ == '__main__':
  main()
