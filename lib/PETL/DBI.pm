package PETL::DBI;
use strict;
use warnings;

use DBI                  ();
use Exporter         qw  (import);
use GenericViewer        ();
use Try::Tiny        qw  (try catch finally);
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

	} catch {

	  my $e = $_;

	  #  mark the result set as finished if exception thrown
	  # then propagate the exception
	  push @DB::typeahead, 'x $e';
	  $DB::single = 1;
	  die $e if defined $e;

	} finally {

            # clear the reference to the statement handle
            $sth = undef;

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
