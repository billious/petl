package PETL::SHA1;
use 5.10.0;
use strict;
use warnings;
no warnings qw(uninitialized);

use Class::Multimethods ;
use Digest::SHA1;
use PETL qw (:all) ;

# exported symbols
our @EXPORT_OK = 
    qw(
          sha1
  );

our %EXPORT_TAGS = 
    (
        all => \@EXPORT_OK,
    );

# SHA1 keyword
multimethod sha1 => qw(Publisher) => sub {
    my Publisher $pub = shift;

    return _simple
        ( sub {
              my GenericViewer $v = shift;

              my $digest = Digest::SHA1->new();
              my $scanner = sub {
                  my GenericViewer $ig = shift;
                  my $cells = shift;

                  $digest->add( join( $;, @$cells ) );
              };

              # accumulate output in digest
              $pub->( 
                  _gv(
                      on_header => $scanner,
                      on_row    => $scanner,
                  )
              );

              # write digest result to viewer
              column( sha1 => [ $digest->digest ] )->( $v );
          }
        );
};


__PACKAGE__;
