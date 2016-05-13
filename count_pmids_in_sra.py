#!/bin/env python2.7
import sys
import re 
import gzip
import glob

import lxml.etree as xparser
from xml import sax


#used to extract out SRX's and associated XREF_LINKs to PubMed
class Srx2PmidExtractor(sax.handler.ContentHandler):
    def parseString(self, string):
        self.srx2pmids={}
        self.srx=None
        #flags for setting up to get pmid
        self.DB=False
        self.pubmed=False
        self.pmid=False
        sax.parseString(string, self)
        return self.srx2pmids

    def startElement(self, name, attrs):
        #reset all flags if we have a new package
        if name == 'EXPERIMENT_PACKAGE':
          self.pmid=False
          self.pubmed=False
          self.DB=False
        if name == 'EXPERIMENT':
          self.srx = attrs.getValue('accession')
          self.srx2pmids[self.srx]=set()
        if name == 'DB':
          self.DB=True
          self.pubmed = False
          self.pmid = False
        if name == 'ID' and self.pubmed and not self.DB:
          self.pmid=True
          self.pubmed=False

    def characters(self, content):
        if self.pmid and not self.pubmed and not self.DB:
          self.srx2pmids[self.srx].add(content)
          self.pmid=False
        if self.DB and content == 'pubmed':
          self.pubmed=True
          self.DB=False

def main():
  files_ = glob.glob('*.gz')
  for f in files_:
    with gzip.open(f,"r") as fin:
      xml = fin.read()
      srx2pmids = Srx2PmidExtractor().parseString(xml)
      for (srx,pmids) in srx2pmids.iteritems():
        if len(pmids) > 0:
          sys.stdout.write("%s\t%s\n" % (srx,";".join(sorted(pmids))))

if __name__ == '__main__':
  main()
