#!/usr/bin/perl
use warnings;
use strict;

my $line;
while( $line = <>   ){
  my @words = split(/\s+/, $line);
  my $word;
  
  foreach $word(@words){
    print $word,"\t",1,"\n";
 }
}


