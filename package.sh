export DIR=cwilks_cs466_final_project
mkdir $DIR
rsync -av *.py $DIR/
rsync -av *.pl $DIR/
rsync -av README.md $DIR/
rsync -av test_queries.sh $DIR/
rsync -av common_words $DIR/
rsync -av refFlat.hg38.txt.sorted $DIR/
rsync -Lav pubmed_map.tsv $DIR/
rsync -Lav sra_map.tsv $DIR/
rsync -Lav env.sh $DIR/
rm $DIR/bioirws.py
rm $DIR/sample_query.py
tar -cvf ${DIR}.tar $DIR
bzip2 ${DIR}.tar
