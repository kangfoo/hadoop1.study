#!/usr/bin/perl
use warnings;
use strict;

my $line;
my %table;
while( $line =<> ){
	chomp($line);
	my ($k, $v) = split(/\t/, $line) ;
	if( $table{$k}  ){
		$table{$k} = $table{$k} + $v;
	}else{
		$table{$k} = $v;
	}
}

my $key;
foreach $key(keys(%table)){
	print $key, "\t", $table{$key},"\n",
}
