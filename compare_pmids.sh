cat $1 | perl -ne 'chomp; $s=$_; next if($s=~/^$/); print "$s\n";' > $1.noempties
cut -f 2,5 $1.noempties | perl -ne 'chomp; $s=$_; next if($s=~/\t$/); print "$s\n";' > $1.noempties.srx2pmids
cut -f 1 $1.noempties.srx2pmids | sort -u > $1.noempties.srx2pmids.uniq_srx
cut -f 2 $1.noempties.srx2pmids | perl -ne 'chomp; @f=split(/;/,$_); for $x (@f) { print "$x\n";}' | sort -u > $1.noempties.srx2pmids.uniq_pmid
