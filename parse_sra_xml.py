#!/bin/env python2.7
import sys
import re 
import gzip
import glob

import lxml.etree as xparser
from xml import sax

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField
from lucene_indexer import LuceneIndexer

from IdentityExtractor import IdentifierExtracter
from count_pmids_in_sra import Srx2PmidExtractor

#expanded from http://stackoverflow.com/questions/17530471/python-get-all-text-from-an-xml-document
#only used for the raw exraction of all text, space delimited for word searching
class FullXML2TextHandler(sax.handler.ContentHandler):
    def parseString(self, string):
        self.text = []
        sax.parseString(string, self)
        return ' '.join(self.text)

    def characters(self, data):
        self.text.append(data)
   
    #this was a necessary addition to pick up text in attributes 
    def startElement(self, name, attrs):
        for name in attrs.getNames():
          self.text.append(attrs.getValue(name))

hugo_genenamesF = 'refFlat.hg38.txt.sorted'

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
        #VALUE
        #actaully only add these to the first field which is all the attributes concatenated by spaces
        fields[0].append("%s$%s" % (child[0].text,child[1].text))
    elif child.text != None:
      fieldname = "%s_%s" % (top_e,child.tag)
      header.append([fieldname,TextField])
      if fieldname not in fields_seen: 
        fields_seen.add(fieldname)
      fields.append(child.text)
      fields[0].append(child.text)

#here we just add all the attributes *of an element* to the "all" field (index 0)
#NOTE: not to be confused with "*_ATTRIBUTES" elements, handled elsewhere
def add_element_attributes(element,attribute_names,fields,accessions):
  for attribute_name in attribute_names:
    value = element.get(attribute_name)
    if value != None:
      fields[0].append("%s$%s" % (attribute_name,value))
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

def parse_raw_text_for_ids(exp_xml,ie):
  xmlstring = xparser.tostring(exp_xml,pretty_print=True)
  #srx2pmids = Srx2PmidExtractor().parseString(xmlstring)
  raw_text = FullXML2TextHandler().parseString(xmlstring)
  (genes,accessions,pmids) = ie.extract_identifiers("NA",0,raw_text)
  #srx = srx2pmids.keys()[0]
  #pmids_ = srx2pmids[srx]
  #pmids_.update(set(pmids))
  #return (genes,accessions,pmids_,srx)
  return (genes,accessions,pmids)

#this is the main parsing method
#parses one EXPERIMENT_PACKAGE section (one document in lucene)
def parse_exp(exp_xml,li,ie,srx2pmids):
  fields = [[]]
  header = [['all',TextField]]
  accessions = set()
  pmids = set()
  accession = 'NA'

  #extract gene names and any free text pmids
  (found_genes,accessions,pmids_) = parse_raw_text_for_ids(exp_xml,ie)
  map(lambda z: fields[0].append("%s$%s" % ('pubmed',z)),pmids_)
  pmids.update(set(pmids_))
  #pmids.update(pmids_)

  for (top_e,subs) in elements.iteritems():
    e = exp_xml.find(top_e)
    #make sure to grab all pubmed id links
    if top_e == 'STUDY' and e != None:
      xlinks = e.findall(".//XREF_LINK")
      for xlink in xlinks:
        if len(xlink) > 1 and xlink[0].text == 'pubmed' and xlink[1].text != None:
          fields[0].append("%s$%s" % (xlink[0].text,xlink[1].text))
          pmids.add(xlink[1].text)
    if e == None:
      continue
    #only grab the child elements and their children (where applicable) that we care about
    for (k,v) in subs.iteritems():
      #get the xml attributes of the current element (top_e)
      if k == 'attrs':
        for attr_ in v:
          attr = e.get(attr_)
          if attr_ == 'accession':
            accessions.add(attr)
            if top_e == 'EXPERIMENT':
              accession = attr
              if attr in srx2pmids:
                pmids.update(srx2pmids[attr]) 
                del srx2pmids[attr]
          if attr != None:
            fields.append(attr)
            fieldname = "%s_%s" % (top_e,attr_)
            header.append([fieldname,StringField])
            if fieldname not in fields_seen: 
              fields_seen.add(fieldname)
            fields[0].append(attr)
      #sub elements and their children
      else:
          sube = e.find(".//%s" % k)
          if sube == None:
            continue
          #get the first sub-level children  of this sub element
          if v == 2:
            process_sub_children(sube,fields,header,top_e)
          #otherwise parse this sub element's text
          elif sube.text != None:
            fields.append(sube.text)
            #need to prefix with the top element name since there will be multiples of some subchildren (e.g. "TITLE")
            fieldname = "%s_%s" % (top_e,sube.tag)
            header.append([fieldname,TextField])
            if fieldname not in fields_seen: 
              fields_seen.add(fieldname)
            fields[0].append(sube.text)

  parse_run_set(exp_xml,header,fields,accessions)

  genes_sorted = ";".join(sorted(found_genes))
  genes_sorted = genes_sorted.rstrip()
  fields.append(genes_sorted)
  header.append(['genes',TextField])
  pmids_sorted = ";".join(sorted(pmids))
  pmids_sorted = pmids_sorted.rstrip()
  fields.append(pmids_sorted)
  header.append(['pmids',TextField])
  fields.append(xparser.tostring(exp_xml))
  header.append(['raw',TextField])


  if len(fields) > 0:
    fields[0] = ' '.join(fields[0])
  pmids_ = ""
  print("adding_document\t%s\t%s\t%s\t%s" % (accession,";".join(sorted(accessions)),genes_sorted,pmids_sorted))
  li.add_document(fields,header,accession)
          
def parse_sra_chunk(xml,li,ie,srx2pmids):
  root = xparser.fromstring(xml)
  exps = root.findall('EXPERIMENT_PACKAGE')
  for exp in exps:
    parse_exp(exp,li,ie,srx2pmids) 

def main():
  li = LuceneIndexer("./lucene_sra_index4")
  ie = IdentifierExtracter(hugo_genenamesF,gene_filter=re.compile(r'[\-\d]'),filter_stopwords=True)
  files_ = glob.glob('*.gz')
  for f in files_:
    with gzip.open(f,"r") as fin:
      xml = fin.read()
      srx2pmids = Srx2PmidExtractor().parseString(xml)
      parse_sra_chunk(xml,li,ie,srx2pmids)
      for missed_srx in srx2pmids:
        sys.stderr.write("MISSED_SRX_WITH_PMIDS\t%s\t%s\n" % (missed_srx,";".join(srx2pmids[missed_srx])))
  li.close()
  for fieldname in fields_seen:
    sys.stdout.write("%s\n" % (fieldname)) 

if __name__ == '__main__':
  main()
