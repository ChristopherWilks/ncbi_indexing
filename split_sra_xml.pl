#!/usr/bin/perl
use strict;


#all data is split by '<EXPERIMENT_PACKAGE>'
my %map=('EXPERIMENT_PACKAGE'=>1,EXPERIMENT=>1,RUN=>1,SUBMISSION=>1,STUDY=>1,SAMPLE=>1,EXPERIMENT_SET=>1,RUN_SET=>1,SAMPLE_SET=>1);

my $tab = "";
while(my $line = <STDIN>)
{
  chomp($line);
  my @e = split(/></,$line);
  #my @e = split(//,$line);
  foreach my $e (@e)
  {
    $e=~s/^<//; 
    $e=~s/>$//; 
    my $tab_old = $tab;
    #$e =~ /^([^"\s}+)/;
    #my $first = $1;
    #ending
    if($e =~ /^\/([^"\s]+)/ && $map{$1})
    {
      $tab=~s/  $//;
      print "$tab<$e>\n";
    }
    elsif($e =~ /^([^"\s]+)/ && $map{$1})
    {
      print "$tab<$e>\n";
      $tab.="  ";
    }
    else
    {
      print "$tab<$e>\n";
    }
  }
}
