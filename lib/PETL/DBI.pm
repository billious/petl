package PETL::DBI;
use strict;
use warnings;

use Class::Multimethods    ;
use DBI                  ();
use Exporter         qw  (import);
use GenericViewer        ();
use Publisher            ();
use Try::Tiny        qw  (try catch finally);

our @ISA = qw( Publisher );

our @EXPORT_OK = qw(
                       statement
               );

sub statement {
    # Accept DBI::st, return publisher over DBI::st results
    my DBI::st  $sth    = shift;

    return PETL::publisher 
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

                # clear the reference to the statement handle
                $sth = undef;
                
            };
        }
      );
};

##############################################################################
# EXPORT TAGS
##############################################################################
our %EXPORT_TAGS = 
  (
   all => \@EXPORT_OK,
  );


return __PACKAGE__;


