#!/usr/bin/perl
use strict;

use Test::EasyBish;

# Declare the modules needed for the test
required_modules
    qw(
          PETL
  );

{
    # allocate space for the table returned from PETL, and specify the rows that will be stored
    my $lol;
    my @rows  = ( [1..3], [4..6] );

    scenario
        {
            # test that choose works properly: choose two columns from three column table
            ## scenario 'choose keyword returns subset of columns', 
            title( "'choose' keyword returns subset of columns" );

            ## given 'table with one row and 3 columns', {
            my $table = PETL::table( [ ['a'..'c'], @rows ] );
            ## },

            ## when 'choose arrayref of two existing columns of table', {
            my $choice = [ 'a', 'c' ];
            $lol = PETL::lol( PETL::choose( $choice, $table ) );
            ## },

            ## then 'table with one row and 2 columns returned, {
            ok( @{$lol->[0]} == @$choice, 'correct number of columns returned' );
        };

    scenario
        {
            # verify that correct number of rows returned
            title( 'correct number of rows returned' );
            ok( @rows+1 == @$lol, title()    );
        }; 
}


