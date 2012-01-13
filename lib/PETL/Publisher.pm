package PETL::Publisher;

use strict;

use Class::Multimethods;
use Scalar::Util ();
use PETL::Debug;
use PETL::Viewer;

my @PARMS = qw(on_end on_header on_row on_start publisher);
my $DEFAULT = {};
@$DEFAULT{@PARMS} = map sub {}, @PARMS;

sub new {
    # construct a new PETL::Publisher
    my ($invc) = shift;

    my $parms =
        {
            # use defaults
            %$DEFAULT,

            # default pass-thrus
            on_header => sub {
                my ($v, $header) = @_;
                $v->add_header( $header );
            },
            on_row    => sub {
                my ($v, $row) = @_;
                $v->add_row( $row );
            },

            # which are overwritten by parameterized arguments
            @_
        };

    my ($on_end, $on_header, $on_row, $on_start, $publisher) = @$parms{@PARMS};

    my $self = sub {
        # accept a viewer
        my PETL::Viewer $viewer = shift;

        # construct adapter to delegate to handlers defined in constructor
        my PETL::Viewer $filter = PETL::Viewer->new
            ( on_header => sub {
                  my ($ig, $header) = @_;
                  $on_header->( $viewer, $header );
              },
              on_row    => sub {
                  my ($ig, $row) = @_;
                  $on_row->( $viewer, $row );
              },
          );

        # invoke start handler, publisher, end handler in turn
        for my $handler ( $on_start, $publisher, $on_end ) {
            $handler->( $filter );
        }
    };

    bless $self => ( ref $invc || $invc );
    return $self;
}

no PETL::Debug;
__PACKAGE__;

##############################################################################
package PETL::Publisher::Simple;

our @ISA = qw(PETL::Publisher);

sub new {
  my ($invc) = shift;
  my $self = shift;
  return bless $self, (ref $invc||$invc);
}

no PETL::Debug;
__PACKAGE__;


##############################################################################
package PETL::Publisher::Util;

use Class::Multimethods (multimethod resolve_no_match);
use Exporter;

our @EXPORT = qw(
                    publisher
            );

multimethod publisher => qw(CODE) => sub {
    # bless a code reference into a PETL::Publisher::Simple
    my $callback = shift;
    return PETL::Publisher::Simple->new( $callback );
};

resolve_no_match publisher => sub {
    # publisher with anything else is a synonym for PETL::Publisher->new( @_ )
    return PETL::Publisher->new( @_ );
};


no Class::Multimethods;
__PACKAGE__;
