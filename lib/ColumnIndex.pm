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

sub get_omit_cb {
    # returns a callback that will return all the fields EXCEPT the ones passed in
    my ($self, @omit_fields) = @_;

    my $cb;
    return sub {
        my ($ra_cells) = @_;

        # generate the callback if necessary
        unless ( $cb ) {
            my $s  = $self;
            my $rh = { %$s };
            delete @{$rh}{ @omit_fields };
            my @keep = sort { $s->{$a} <=> $s->{$b} }
                keys %$rh;
            $cb = $self->get_cb( @keep );
        }

        # execute the callback
        return $cb->( $ra_cells );
    };
}

my @tests;
sub proof (&) {
    push @tests, @_;
}

proof {
    # TEST: get_omit_cb
    my $ci    = __PACKAGE__->new;
    my $no_b  = $ci->get_omit_cb( 'b' );
    my $cells = [ 'a' .. 'c' ];
    $ci->add_header( $cells );
    Test::More::is_deeply( [ $no_b->( $cells ) ], [ 'a', 'c' ], 'should be able to omit columns by name' );
};

if( Test::More->can('ok') ) {
    &$_ for @tests;
    Test::More::done_testing();
}


1;
