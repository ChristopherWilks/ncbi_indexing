#!/bin/env python2.7
#Embedded file name: /data2/cs466/final/lucene_indexer.py

import sys
import lucene
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.document import Document, Field, IntField, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
LUCENE_TYPES = {'i': IntField,'s': StringField,'t': TextField}

class LuceneIndexer:

    def __init__(self, path_to_save):
        self.path_to_save = path_to_save
        self.num_docs = 0
        lucene.initVM()
        self.indexDir = SimpleFSDirectory(File(self.path_to_save))
        self.analyzer = StandardAnalyzer(Version.LUCENE_4_10_1)
        self.analyzer2 = WhitespaceAnalyzer(Version.LUCENE_4_10_1)
        self.writerConfig = IndexWriterConfig(Version.LUCENE_4_10_1, self.analyzer2)
        self.writer = IndexWriter(self.indexDir, self.writerConfig)

    def add_document(self, fields, header, id_):
        doc = Document()
        if len(fields) > len(header):
            sys.stderr.write('SKIPPED_DOC\tunexpected_num_lines\t%s\n' % str(id_))
            for field in fields:
                sys.stderr.write('%s\n' % field)
            return
        for idx, field in enumerate(fields):
            fname, fieldtype = header[idx]
            if fieldtype is IntField:
                field = int(field)
            doc.add(fieldtype(fname, field, Field.Store.YES))
        self.writer.addDocument(doc)
        self.num_docs += 1

    def close(self):
        print 'Indexed %d lines from stdin (%d docs in index)' % (self.num_docs, self.writer.numDocs())
        self.writer.close()
