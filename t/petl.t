#!/usr/bin/perl
use strict;

use Test::More;


BEGIN { 
  # needs to compile
  use_ok( 'PETL' );
}

# test that choose works properly: choose two columns from three column table
## scenario 'choose keyword returns subset of columns', 
{
  ## given 'table with one row and 3 columns', {
  my $table = PETL::table( [['a'..'c'], [1..3], [4..6]] );
  ## },
  ## when 'choose arrayref of two existing columns of table', {
  my $lol = PETL::lol( ETL::choose( ['a','c'], $table ) );
  ## },
  ## then 'table with one row and 2 columns returned, {
  my $num_cols = scalar( @{$lol->[0]} );
  ## } 
  ok( $num_cols == 2 && @$lol == 1, 'choose keyword of existing columns returns subset of columns' );


}


# testing is complete
done_testing();
