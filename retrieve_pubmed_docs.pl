#!/usr/bin/perl
use strict;
use warnings;
use JSON;
use LWP::Simple;
use URI::Escape;

my $MAX_INT=(2^32)-1;
my $RUN_TIME_THRESHOLD=10; #seconds;
my $DELAY=0.5; #3 seconds for querying NCBI
my $ID_MODE=1;
my $ABSTRACTS_MODE=2;
my $SUMMARIES_MODE=3;


#general eutils settings, can be overriden in specific functions
my $eutils='http://eutils.ncbi.nlm.nih.gov/entrez/eutils';
my $esearch = "$eutils/esearch.fcgi";
my $efetch = "$eutils/efetch.fcgi";
my $esummary = "$eutils/esummary.fcgi";

my %mode2endpoint=(2=>$efetch,3=>$esummary);

my $tool='JHUPubMedIndexer';
my $email='chris.wilks@jhu.edu';

my $retmode = 'json';
my $database = 'pubmed';
#use as retmax


my $FORCE_RUN;

my $MODE=shift; #1=IDs, 2=abstracts, 3=summaries
$MODE=1 if(!$MODE);
$FORCE_RUN=shift;

my $fetch_max=10000;
$fetch_max = 500 if($MODE == $SUMMARIES_MODE);

main();

sub last_run
{
  my $now_date = `date +%Y%m%d`;
  chomp($now_date);
  my $now_secs = `date +%s`;
  chomp($now_secs);

  my $last_run_secs;
  my $last_run_date;
  my $diff=$MAX_INT;
  if(-e "$0.last_run")
  { 
    open(IN,"<$0.last_run");
    my $last_run = <IN>;
    chomp($last_run);
    close(IN);
    my ($last_run_secs,$last_run_date)=split(/\t/,$last_run);
    $diff = $now_secs-$last_run_secs;
  }
  return ($last_run_secs,$last_run_date,$now_secs,$now_date,$diff);
}

sub main
{
  my ($last_run_secs,$last_run_date,$now_secs,$now_date,$diff) = last_run();
  die "too close to last date ($last_run_date) and FORCE_RUN is not in effect\n" if($diff < $RUN_TIME_THRESHOLD and !$FORCE_RUN);
  
  open(OUT,">$0.last_run");
  print OUT "$now_secs\t$now_date\n";
  close(OUT);

  my $query = 'human[All Fields]'; #human[All Fields]
  my $query2 = 'human[Title/Abstract]'; #human[Title/Abstract]
  #my $query = '%22human%22%5BAll%20Fields%5D'; #human[All Fields]
  #my $query2 = '%22human%22%5BTitle%2FAbstract%5D'; #human[Title/Abstract]

  my ($count,$ids_)=do_esearch($last_run_secs,$last_run_date,$diff,$query);
  if($MODE==$ID_MODE)
  {
    my $ids=condense_ids($ids_);
    print "$count\t".(scalar @$ids)."\n";
  }
}

sub condense_ids
{
  my $ids = shift;
  my @ids;
  for my $id_a (@$ids)
  {
   for my $id (@$id_a)
   {
    push(@ids,$id);
    print STDERR "$id\n";
   }
  }
  return \@ids; 
}

#based on http://www.ncbi.nlm.nih.gov/books/NBK25498/#chapter3.Application_3_Retrieving_large
sub do_esearch
{
  my ($last_run_secs,$last_run_date,$diff,$query)=@_;

  #from http://www.ncbi.nlm.nih.gov/books/NBK1058/#eutils_esayers-5-4-3
  my %esearch_params=(db=>$database,usehistory=>'y',retmode=>$retmode,retmax=>$fetch_max,term=>$query,tool=>$tool,email=>$email);

  my $ep=\%esearch_params;
  my $url = $esearch."?".(build_url($ep));
  logit("getting $url\n");
  my $output = get($url);
  my $r_ = decode_json($output);

  my @ids;
  my @results;

  my $r = $r_->{"esearchresult"};
  my $ids_a = $r->{"idlist"};
  push(@ids,$ids_a);
  my $count = $r->{"translationstack"}->[0]->{"count"};

  delete $ep->{"term"};
  delete $ep->{"usehistory"};
  $ep->{"WebEnv"}=$r->{"webenv"};
  $ep->{"query_key"}=$r->{"querykey"};
  $ep->{"rettype"}="abstract" if($MODE==$ABSTRACTS_MODE || $MODE==$SUMMARIES_MODE);
  
 
  #grab the rest of the IDs
  #my $fetch_size = $fetch_max; 
  my $fetch_size = 0;
  #for(my $i=$fetch_max; $i < $count; $i+=$fetch_size)
  for(my $i=0; $i < $count; $i+=$fetch_size)
  {
    sleep($DELAY);
    $ep->{"retstart"}=$i;
    logit("getting $i/$count\n");
    my $endpoint = $mode2endpoint{$MODE};
    my $output = retrieve_records($ep,$endpoint);
    parse_output($output,undef,\@results);
    #my $esummary_output = retrieve_records($ep,$esummary);
    #my $efetch_output = retrieve_records($ep,$efetch);
    #parse_output($efetch_output,$esummary_output,\@results);
   
    #use fetch_size so we don't go over count with our step
    $fetch_size = $count-$fetch_max;
    $fetch_size = $fetch_max if($fetch_size > $fetch_max);
  }
  return ($count,\@ids);
}

sub retrieve_records
{
  my $ep = shift;
  my $endpoint = shift;

  my $newurl = build_url($ep);
  my $url = $endpoint."?".$newurl;
  logit("running retrieve_records $url\n");
  return get($url);
}
   
sub parse_output
{
  my $output = shift;
  #my $es_output = shift;
  my $results = shift;
   
  if($MODE==$ABSTRACTS_MODE || $MODE==$SUMMARIES_MODE)
  {
    #parse_abstracts($ef_output,$es_output,$results);
    parse_abstracts($output,$results);
    return;
  }  
  my @ids_ = split(/\n/,$output);
  push(@{$results},\@ids_);
}

#do parsing as a later step for now
sub parse_abstracts
{
  my $output = shift;
  my $results = shift;
 
  #print STDERR "INFO:\n$es_output\nABSTRACT:\n$ef_output\n";
  print STDERR "$output\n";
}      

sub build_url
{
  my $params = shift;
  my $params_url = "";
  map {$params_url.="&$_=".uri_escape($params->{$_});} keys %$params;
  $params_url=~s/^&//;
  return $params_url;
}

sub logit
{
  my $m = shift;
  print "$m";
}
