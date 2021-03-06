#!/bin/bash

curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1" -o cancer.tsv
curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1&full=1" -o cancer_full.tsv
curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=cancer%20TP53&add_ids=1&limit=1000" -o cancer1k.tsv
curl "http://stingray.cs.jhu.edu:8090/cs466/bioir?query=EXPERIMENT_TITLE::cancer%20TP53;;TITLE::cancer%20TP53" -o tp53.tsv
