#!/usr/bin/env perl
use 5.10.0;
use strict;
use warnings;


use DBI;
use Test::EasyBish;
use Storable;
use Try::Tiny;

# Pre-declarations
sub get_anydata_dbh;

# Declare the modules needed for the test
required_modules
    qw(
	PETL
	PETL::DBI
	DBD::AnyData
     );


# import PETL keywords
use PETL qw(:all);

scenario 
  {
    title( 'SQL SELECT from db reproduces table contents' ); 

    # scenario given a dbh handle, we should be able to perform a PETL
    # query to reproduce a LoL structure with similar values
    #
    # since DBD::AnyData seems to alter input data structure we give it a
    # deep clone so the original remains intact for comparison via
    # is_deeply
    my $input_lol = [
		     ['a' .. 'c'], 
		     [  1 ..   3], 
		     [  4 ..   6], 
		     [  7 ..   9],
		    ];
 
    my DBI::db $dbh = get_anydata_dbh();
    $dbh->func( 'test', 'ARRAY', Storable::dclone( $input_lol ), 'ad_import' );
    my $output_lol = lol db $dbh, 'SELECT * FROM test';
    
    is_deeply( $output_lol, $input_lol, title() )
      or diag( explain "got ", $output_lol, "expected ", $input_lol );
  };

scenario 
  {
    title( 'db handle accepts rows from a publisher as bind values for INSERT' );

    # scenario: given a dbh handle, a table, and a SQL insert statement,
    # verify columns are bound when a table is the last argument on the right side of the db statement.
    my $input_lol = [
		     [ 'one', 'two' ],
		     [     3,     4 ],
		     [     5,     6 ],
		     [     7,     8 ],
		    ];

    # create the db handle, and table
    my DBI::db $dbh = get_anydata_dbh();
    $dbh->do( 'CREATE TABLE test (one TEXT, two TEXT)' );

    # perform the insert
    lol db $dbh, 'INSERT INTO test VALUES (?, ?)', table( $input_lol );

    # retrieve the contents of the test table, which should be identical to the $input_lol
    my $output_lol = lol db $dbh, 'SELECT * FROM test';

    # test and report the results
    is_deeply( $output_lol, $input_lol, title() )
      or diag( explain "got ", $output_lol, "expected ", $input_lol );
  };

scenario 
  {
    title( 'db handle accepts rows from a publisher as bind values for SELECT' );

    # scenario: given a dbh handle, a table, a SQL select statement,
    # and a publisher verify that columns from the publisher are
    # iterated and combined with the output of the statement
    my $primes_lol = [
		      [ 'sequence', 'prime_number' ],
		      [          0,              1 ],
		      [          1,              2 ],
		      [          2,              3 ],
		      [          3,              5 ],
		      [          4,              7 ],
		     ];

    # make a table publisher over the data in $primes_lol
    my $primes_pub = table $primes_lol;

    # get handle and create test table
    my DBI::db $dbh = get_anydata_dbh();
    $dbh->do( 'CREATE TABLE primes ( sequence INT, prime_number INT )' );
   
    # populate test table
    lol db $dbh, 'INSERT INTO primes VALUES (?, ?)', $primes_pub;

    # invoke SQL for every row in $primes_pub, and gather results into $output_lol
    my $output_lol = lol db $dbh, 'SELECT sequence, prime_number FROM primes WHERE sequence = ?', 
      choose ['sequence'], $primes_pub;

    # perform the test comparing $output_lol with $primes_lol
    is_deeply( $output_lol, $primes_lol, title() )
      or diag( explain "got ", $output_lol, "expected ", $primes_lol );
  };


exit;

##############################################################################
# SUBROUTINES
##############################################################################
sub get_anydata_dbh {
  # return standard connection to AnyData instance
  return DBI->connect( 'dbi:AnyData(RaiseError=>1):' );
}

