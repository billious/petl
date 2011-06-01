use strict;
package Publisher;

use GenericViewer;
use Scalar::Util ();

my $default = {};
my @parms = qw(on_end on_header on_row on_start publisher);
@$default{@parms} = map eval( 'sub{}' ), @parms;

sub _BREAK_HERE_ { $DB::single = 1 }

sub new {
  my ($invc) = shift;

  my $parms =
    {
     # use defaults
     %$default,
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

  my ($on_end, $on_header, $on_row, $on_start, $publisher) = @$parms{@parms};

  my $self = sub {
    # accept a viewer
    my ($viewer) = @_;

    # construct adapter to delegate to handlers defined in constructor
    my $gv = GenericViewer->new
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
      $handler->( $gv );
    }
  };

  return bless $self => ( ref $invc || $invc );
}

package Publisher::Simple;

our @ISA = qw(Publisher);

sub new {
  my ($invc) = shift;
  my $self = shift;
  return bless $self, (ref $invc||$invc);
}


1;
