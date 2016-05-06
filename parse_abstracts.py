#!/usr/bin/env python2.7
import sys
import re
import gzip

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField

from lucene_indexer import LuceneIndexer
from IdentityExtractor import IdentifierExtracter

start_patt = re.compile(r'^(\d+)\.\s+(.+)$')
spacer_line_patt = re.compile(r'^\s*$')
author_info_patt = re.compile(r'^Author information:\s*$')
end_patt = re.compile(r'^PMID:\s+(\d+).*$')

hugo_genenamesF = 'refFlat.hg38.txt.sorted'

#expect: journal,title,authors,author_info,abstract,and pmid at least
#if we get additional fields, well stick them on the end of the abstract proper so as to avoid any specialized field handling
HEADER = [['JOURNAL',TextField],['TITLE',TextField],['AUTHORS',TextField],['AUTHOR_INFO',TextField],['ABSTRACT',TextField],['PMID',StringField],['raw',TextField],['genes',TextField],['accessions',TextField]]
ABSTRACT_INDEX=4

BOTH_MODE=1 #default
LUCENE_MODE=2
EXTRACT_IDS_MODE=3

def process_abstract(processors,pmid,fields,counter,mode):
  (genes,accessions)=([],[])
  if mode != LUCENE_MODE:
    (genes,accessions) = processors['extractor'].extract_identifiers(pmid,counter,fields[-1])
    sys.stdout.write("IDs\t%s\t%s\t%s\n" % (pmid,";".join(sorted(genes)),";".join(sorted(accessions))))
  if mode != EXTRACT_IDS_MODE:
    print("Inserting %s %d into lucene %d %d:" % (pmid,counter,len(genes),len(accessions)))
    fields.append(";".join(sorted(genes)))
    fields.append(";".join(sorted(accessions)))
    processors['indexer'].add_document(fields,HEADER,pmid)
  #for field in fields:
  #  print(field)

def parse_abstracts(processors,mode,f):
  rindex = ""
  fields = []
  findex = 0
  prev_empty_line = False
  counter=0
  raw = []

  for line in f:
    if spacer_line_patt.search(line):
      prev_empty_line = True
      continue
    line = line.rstrip()
    #track all lines in almost raw form (replaces line breaks with a space)
    raw.append(line)
    m = start_patt.search(line)
    author_ = author_info_patt.search(line)
    e = end_patt.search(line)
    #skip as if this was a nonline
    if author_:
      continue
    #starts with journal info
    if m and findex == 0:
      rindex = m.group(1)
      journal = m.group(2)
      fields.append(journal)
    #ends with PMID
    elif e and findex > 0:
      pmid = e.group(1)
      fields.append(pmid)
      fields.append(" ".join(raw))
      #do lucene insertion here
      counter+=1
      process_abstract(processors,pmid,fields,counter,mode)
      findex = 0
      fields = []
      raw=[]
    elif not spacer_line_patt.search(line) and len(fields) > 0: 
      if prev_empty_line and findex < ABSTRACT_INDEX:
        #dont include the "Author information" line
        findex+=1
        fields.append(line)
      else:
        #concatenate multi-line fields
        fields[findex]="%s %s" % (fields[findex],line) 
    prev_empty_line = False


def main():
  if len(sys.argv) < 2:
    sys.stderr.write("need abstracts file to parse\n")
    sys.exit(-1) 
  inputF = sys.argv[1]
  mode = BOTH_MODE
  processors = {}

  if len(sys.argv) > 2:
    mode = sys.argv[2]
  if mode != EXTRACT_IDS_MODE:
    processors['indexer']=LuceneIndexer("./lucene_pubmed_index2")
  if mode != LUCENE_MODE:
    processors['extractor']=IdentifierExtracter(hugo_genenamesF,gene_filter=re.compile(r'[\-\d]'),filter_stopwords=True)

  f=None
  if inputF[-3:] == '.gz':
    f = gzip.open(inputF,"r")
  else:
    f = open(inputF,"r")
  
  parse_abstracts(processors,mode,f)
  
  if mode != EXTRACT_IDS_MODE:
    li.close()
  f.close()


if __name__ == '__main__':
  main()
