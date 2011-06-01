package PETL::DBI;
use strict;
use warnings;

use DBI                  ();
use Exporter         qw  (import);
use GenericViewer        ();
use Try::Tiny            ();
use Publisher            ();

our @ISA = qw( Publisher );

our @EXPORT_OK = qw(
		     statement
		  );

sub statement {
  # Accept DBI::st, return publisher over DBI::st results
  my DBI::st  $sth    = shift;

  return Publisher::Simple->new
    ( sub {
	my GenericViewer $v = shift;

	try {

	  # transfer the column names and all rows of the result set
	  # to the viewer
	  $v->add_header([ @{$sth->{NAME}} ]);
	  while ( my $row = $sth->fetchrow_arrayref ) {
	    $v->add_row( $row );
	  }

	} finally {

	  # always mark the result set as finished
	  $sth->finish();

	};

      }
    );
}

##############################################################################
# EXPORT TAGS
##############################################################################
our %EXPORT_TAGS = 
  (
   all => \@EXPORT_OK,
  );


return __PACKAGE__;
