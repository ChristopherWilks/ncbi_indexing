#!/bin/env python2.7
import sys
import re
import gzip

import lucene
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField

from lucene_indexer import LuceneIndexer

start_patt = re.compile(r'^(\d+)\.\s+(.+)$')
spacer_line_patt = re.compile(r'^\s*$')
author_info_patt = re.compile(r'^Author information:\s*$')
end_patt = re.compile(r'^PMID:\s+(\d+).*$')

#expect: journal,title,authors,author_info,abstract,and pmid at least
#if we get additional fields, well stick them on the end of the abstract proper so as to avoid any specialized field handling
#HEADER = ['journal_t','title_s','authors_t','author_info_t','abstract_t','pmid_s','copyright_t','extra_t']
HEADER = [['JOURNAL',TextField],['TITLE',TextField],['AUTHORS',TextField],['AUTHOR_INFO',TextField],['ABSTRACT',TextField],['PMID',StringField],['raw',TextField]]
BASE_NUM_FIELDS=len(HEADER)
ABSTRACT_INDEX=4

def insert_into_lucene(li,pmid,fields,counter):
  print("Inserting %s %d into lucene:" % (pmid,counter))
  #for field in fields:
  #  print(field)
  li.add_document(fields,HEADER,pmid)

def main():
  inputF = sys.argv[1]
  li = LuceneIndexer("./lucene_pubmed_index")
  rindex = ""
  fields = []
  findex = 0
  prev_empty_line = False

  f=None
  if inputF[-3:] == '.gz':
    f = gzip.open(inputF,"r")
  else:
    f = open(inputF,"r")

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
      insert_into_lucene(li,pmid,fields,counter)
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
        #fields[findex]="%s\n%s" % (fields[findex],line) 
        fields[findex]="%s %s" % (fields[findex],line) 

    prev_empty_line = False

    #prev_empty_line = False
    #if spacer_line_patt.search(line):
    #if current_empty:
      #prev_empty_line = True

  #sys.stderr.write("more than 6 line count %s\n" % (more_than_6_field_count))
  #sys.stderr.write("more than 7 line count %s\n" % (more_than_7_field_count))
  #sys.stderr.write("copy count %s\n" % (copy_count))
  li.close()
  f.close()

if __name__ == '__main__':
  main()
