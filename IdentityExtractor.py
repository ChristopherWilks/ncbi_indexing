#!/usr/bin/env python2.7
import sys
import re

stopwordsF = './common_words'

class IdentifierExtracter():

  def __init__(self,genenamesF,columns=[0,1],gene_filter=None,gene_filter_out=False,filter_stopwords=False):
    self.gene_filter = gene_filter
    self.gene_filter_out = gene_filter_out
    self.stopwords = set()
    if filter_stopwords:
      with open(stopwordsF,"r") as fin:
        for line in fin:
          self.stopwords.add(line.rstrip().upper())
    self.genenamesF = genenamesF
    self.genenames = set()
    with open(self.genenamesF,"r") as fin:
      for line in fin:
        fields = line.rstrip().split("\t")
        #by default we get both the HUGO gene name AND the refseq accession
        for column in columns:
          self.genenames.add(fields[column].upper()) 
    #here we specify the SRA accession regex (matches: submission(A),run(R),sample(S),study(P), and experiment(X))
    #for more information see: http://www.ncbi.nlm.nih.gov/books/NBK56913/
    self.sra_patt = re.compile(r'(SR[ARSPX]\d{6,6})')

  def extract_identifiers(self,id_,counter,raw_text):
    genes = set()
    accessions = set()
    words = re.split("\s+",raw_text)
    for word in words:
      word = word.upper()
      #genename filtering is complicated by the fact
      #that genenames overlap regular English words
      if word in self.genenames and \
        word not in self.stopwords and \
        (self.gene_filter == None or \
          (self.gene_filter.search(word) != None and not self.gene_filter_out) or \
            (self.gene_filter.search(word) == None and self.gene_filter_out)):
        genes.add(word)
      m = self.sra_patt.search(word) 
      if m:
       accessions.add(m.group(1))
    return (genes,accessions) 
