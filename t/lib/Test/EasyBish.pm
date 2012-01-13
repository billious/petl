package Test::EasyBish;

use strict;
use warnings;

use Exporter qw(import);
use Test::More;

# faux package to allow "my Code $ref" declaration
{
    no warnings 'once';
    @Code::ISA = qw(CODE);
}

# we export three subroutines as well as those exported by Test::More
our @EXPORT = qw
    (
        required_modules
        scenario
        title
);
push @EXPORT, @Test::More::EXPORT;

# Test and title storage
my @tests;
my @titles;
my $current;

# predeclarations
sub scenario (&);

sub get_num_tests {
    # return the number of defined tests
    return scalar @tests;
}

sub required_modules {
    # add a use_ok scenario for each module
    for my $module ( @_ ) {
        scenario { use_ok( $module ) };
        title( "$module module available and compiles" );
    }
}

sub run_tests {
    # execute the tests -- use shared package variable '$current' to
    # reference the test
    for $current ( @tests ) {
        &$current();
    }
}

sub scenario (&) {
    # define and register a test
    my Code $callback = shift;
    push @tests, Test::EasyBish::Scenario->new( $callback );
    $current = $tests[-1];
}

sub title {
    # access or modify the title of the current scenario
    return $current->title( @_ );
}

END {
    # register the number of tests to run
    Test::More::plan( tests => get_num_tests() );

    # run the tests
    run_tests();

    # complete testing
    done_testing( get_num_tests() );
}

1;

  ########################################
 ##
### Test::EasyBish::Scenario
###
###    inner class to hold Scenario attibutes
###
##########################################
##
#
package Test::EasyBish::Scenario;
use Scalar::Util ();

my %title_for;

my $refaddr = Scalar::Util->can( 'refaddr' );

sub new {
    my $invc          = shift;
    my Code $callback = shift;

    my $package = ref( $invc ) || $invc;
    my $self =  bless $callback => $package;
    $self->title( 'unspecified' );
    return $self;
}

sub title {
    my $self = shift;

    # setter
    if( @_ ) {
        $title_for{ &$refaddr( $self ) } = shift;
        return;
    }

    return $title_for{ &$refaddr( $self ) };
}

__PACKAGE__;


=head1 NAME

Test::EasyBish

=head1 DESCRIPTION

EasyB-like executable content semantics layered on top of Test::More.

The module collects scenarios (scenario keyword) and module load tests
(required_modules keyword) into a test plan, then executes the
scenarios as late as possible, and in the order the scenarios were
received.

The module also provides a convenience accessor to the scenario title
(title keyword).

=head1 SYNOPSIS

  use Test::EasyBish;

  required_modules qw( MyModuleBeingTested );

  scenario 
    {
      title "ensure addition of 1 and 1 equals 2";

      is( 1+1, 2, title )
        or diag( "1 plus 1 does not seem to equal 2" );
    };


=cut
