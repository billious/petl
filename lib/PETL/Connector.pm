package PETL::Connector;
use strict;
use warnings;

use Carp     qw( carp confess croak );
use Exporter qw( import ) ;

our @EXPORT = qw
    (
        connector
    );

sub new {
    # bless a code reference into
    my $invc      = shift;
    my $connector = shift;
    bless $connector => ( ref $invc || $invc );
}

sub connector {
    # factory function to return a PETL::Connector
    my $connector = shift;
    return __PACKAGE__->new( $connector );
}

sub connect {
    my PETL::Connector $self = shift;
    return &$self;
}

no warnings;
__PACKAGE__;
