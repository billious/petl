package GenericViewer;

use strict;
use constant ON_HEADER  => 0;
use constant ON_ROW     => 1;
use constant HAS_HEADER => 2;

sub new {
  my $invc = shift;
  my %params =
    (
     on_header => sub {},
     on_row    => sub {},
     @_
    );

  my $self = [ @params{'on_header', 'on_row'}, 0 ];
  return bless $self => (ref $invc || $invc);
}

sub add_header {
  # delegate header to registered handler
  my( $self, $ra_header ) = @_;
  return if $self->has_header;
  $self->[ON_HEADER]->( $self, $ra_header );
  $self->[HAS_HEADER] = 1;
}

sub add_row {
  # delegate row to registered handler
  my( $self, $ra_row ) = @_;
  $self->[ON_ROW]->( $self, $ra_row );
}

sub has_header {
    # return boolean whether we've accepted a header
    my $self = shift;
    return $self->[HAS_HEADER];
}

sub set_on_header {
  # register new header handler
  my( $self, $handler ) = @_;
  $self->[ON_HEADER] = $handler;
  return $self;
}

sub set_on_row {
  # register new row handler
  my( $self, $handler ) = @_;
  $self->[ON_ROW] = $handler;
  return $self;
}

1;


=head1 NAME

GenericViewer

=head1 DESCRIPTION

Prototype class for accepting table events

=head1 SYNOPSIS

  use GenericViewer;

  my $common = sub {
    my ($self, $ra_cells) = @_;
    print join( qq(\t), @$ra_cells ), qq(\n);
  };

  my $gv = GenericViewer->new
    (
     on_header => $common,
     on_row    => $common,
    );

  $gv->add_header([ 'A' .. 'C' ]);
  $gv->add_row   ([   1 ..   3 ]);
  $gv->add_row   ([   4 ..   6 ]);

=cut
