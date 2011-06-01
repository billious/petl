package ColumnIndex;
use strict;


sub new {
  my ($invc) = shift;

  return bless {} => (ref $invc || $invc);
}

sub add_header {
  # accepts reference to an array of field names and establishes the
  # mapping between field name and ordinal position in the array
  my ($self, $ra_header) = @_;

  @{$self}{@$ra_header} = 0 .. $#$ra_header;
}

sub get_cb {
  # returns callback that will always return the desired columns
  my ($self, @fields) = @_;

  my $splice;
  return sub {
    my ($ra_cells) = @_;

    # compile splice on first invocation
    unless( $splice ) {
      $splice = [];

      # throw exception if any requested columns are missing
      if( my @missing = grep !exists($self->{$_}), @fields ) {
	local $" = ", ";
	die "FATAL: Requested columns missing: @missing\n";
      }
      @$splice = @{$self}{@fields};
    }

    # return the requested column values
    return @{$ra_cells}[@$splice];
  };
}

1;
