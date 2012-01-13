#!/usr/bin/env perl
use 5.10.0;
use strict;
use warnings;

use DBI;
use Test::EasyBish;
use Try::Tiny;

# Pre-declarations
sub lol;
sub regex;
sub table;

# Declare the modules needed for the test
required_modules
  qw(
      PETL
    );

# import PETL keywords at runtime
require PETL;
PETL->import( ':all' );

# the input of a single column of white space delimited numbers and a non-matching text row
my $input = table [
		   [ 'column' ],
		   [ '1 2'    ],
		   [ '3 4'    ],
		   [ 'should not match' ],
		  ];

# what we expect to be returned
my $expected = [ 
		[ 'field000', 'field001', ],
		[          1,          2, ],
		[          3,          4, ],
	       ];

# verify proper operation of regex( Regex, Publisher )
scenario
  {
    title "the regex Regex, Publisher captures matches";

    # a compiled regular expression
    my $regex = qr/^(\d+)\s+(\d+)$/;

    my $result = lol regex $regex, $input;
    is_deeply( $result, $expected, title )
      or diag( explain "expected ", $expected, "got ", $result );
  };
