Chris Wilks
CS466
Spring 2016
Final Project README
--------------------

Please refer to Appendix A in the Final Project Report accompanying this README.
It contains examples of how to query the webservice that I am running to front
access to the BioIR query tool and associated datasets.
It also lists the majority of relevant fields indexed by Lucene for both SRA
and PubMed.

The following is a list of the files and a brief description of each.

With the exception of the last entry (included as a dependency only),
part of the SAX handler code, and part of the Lucene search code (which
were adapted from SO posts and/or was developed previously)
all the code here was developed for this project by me.

query.py
--------
Main "driver" script for the query interface to the Lucene and related indices.
This has the same interface whether called directly form the command line or
via the webservices.

To run you must have PyLucene 4.10.1 or higher
(though this is the only version tested on).

The following examples are provided in the accompanying shell script:
test_queries.sh

example query without specific field names but with additional IDs:

python query.py "query=cancer TP53&add_ids=1"


However, given the potential difficulty in setting up a local instance,
I suggest using the webservice version (same query interface) instead:

curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1" -o cancer.tsv


Return format is (TAB delimited where spaces are):
<PMID|EXPERIMENT_accession> primary_id score [additional_ids/accessions/genenames] [full (raw) doc]

To retrieve the full docs along side the ids add a "full=1" to the query

curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1&full=1" -o cancer_full.tsv

Also, the current limit to retrieve from Lucene is 100 records per database (so 200 max)
this can be changed with adding the parameter "limit=X":

curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1&limit=1000" -o cancer1k.tsv

example query user field names for both SRA (EXPERIMENT_TITLE) and 
PubMed (TITLE) and a gene name (TP53) but without the associated PMIDS/accessions/Genes:

curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=EXPERIMENT_TITLE::cancer%20TP53;;TITLE::cancer%20TP53" -o tp53.tsv


retrieve_pubmed_docs.pl
-----------------------
To run (abstracts mode):

perl retrieve_pubmed_docs.pl 2 1 2>&1 1> run.status | gzip > pubmed_abstracts.gz

Main script to interface with NCBI EUtils webservices for retrieving 
both PubMed abstracts/summaries and SRA metadata.  
Essentially this is my version of the the web spider/gatherer.  

IdentityExtractor.py
--------------------
Module for parsing out gene names (based on HUGO gene names), 
SRA accessions, and PubMed IDs in raw text or XML text.

count_pmids_in_sra.py
---------------------
Simple SAX based stream parser of SRA XML to extract 
all explicit references to PubMed IDs (PMIDS) in the SRA metadata
and link them to SRX (experiment) accessions.
Used to check the third evaluation criterion from the proposal (overlap
of SRA-PubMed links between my combination of explicit and implicit parsing
vs. just an orthogonal, explicit approach).

parse_abstracts.py
------------------
To run:

python parse_abstracts.py pubmed_abstracts.gz > pubmed_map.tsv

Parses PubMed abstracts from their multi-line text format into
individual Lucene fields (JOURNAL, TITLE, AUTHORS, etc...).  Also
leverages the IdentityExtractor module (above) to extract genes and
SRA accessions from the raw text.
In addition to building a Lucene index, it also outputs a TSV of
PMID<->GENE NAME(s)<->SRA ACCESSION(s) mapping which is used by query.py.

parse_sra_xml.py
----------------
To run (must be in a directory which has one or more gzipped
files containing raw SRA XML metadata on single lines):

python parse_sra_xml.py > sra_map.tsv

Parses SRA XML data downloaded in bulk and compressed in one or more
gzipped files in the current directory. Utilizes IdentityExtractor.py
to parse out gene names, additional SRA accessions, and PubMed IDs.
Also makes use of count_pmids_in_sra.py to further extract explicit
references to PubMed IDs missed by it's other extraction methods.
In addition to building a Lucene index, it also outputs a TSV of
SRX ACCESSION<->additional SRA ACCESSION(s)<->GENE NAME(s)<->PMID(s)
mapping which is used by query.py.

split_sra_xml.pl
----------------
First pass parser for raw SRA XML data.
Mainly to split it up into newlines and tabs for human readability
for analysis of which fields were relevant.

lucene_indexer.py
-----------------
Included only as a dependency.
Generic Lucene indexing code from other's work/previous projects.
