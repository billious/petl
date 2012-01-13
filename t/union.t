#!/usr/bin/env perl
use 5.10.0;
use strict;
use warnings;

use Test::EasyBish;
use Try::Tiny;

# Pre-declarations
sub table;
sub viewer;

# Declare the modules needed for the test
required_modules
  qw(
      PETL
    );

# import PETL keywords at runtime
require PETL;
PETL->import( ':all' );

# three column table
my $a = table 
    [
        ['a' .. 'c'],
        ['a' .. 'c'],
    ];

# two column table
my $b = table
    [
        ['a' .. 'b'],
        ['a' .. 'b'],
    ];

# union unit
my $unit = union( $a, $b );

# verify union produces consistent number of fields
scenario
    {
        title "union( Publisher, Publisher ) has consistent number of fields";


        my $seen = {};
        my $common = sub {
            my ($v, $cells) = @_;
            $seen->{ scalar(@$cells) }++;
        };

        my $v = viewer
            (
                on_header => $common,
                on_row    => $common,
            );

        $unit->( $v );

        is( keys( %$seen ), 1, title )
            or diag explain( "inconsistent number of rows seen", [ sort keys %$seen ] );
    };

# verify union only sends header once
scenario
  {
    title "union( Publisher, Publisher ) sends only one header";

    my $num_header = 0;
    my $v = viewer on_header => sub { $num_header++ };
    $unit->( $v );

    is( $num_header, 1, title )
        or diag( "saw $num_header header rows instead of one header only." );
  };
