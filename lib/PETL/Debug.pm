package PETL::Debug;

use strict;
use warnings;

our @EXPORT = qw(
                    _BREAK_HERE_
                    _DEBUGGER_
                    _TYPE_AHEAD_
            );

use constant IN_DEBUGGER => DB->can( 'DB' );

sub _BREAK_HERE_ {
    # description: execute a break point with optional debugger commands

    # do nothing unless running in the debugger
    return unless IN_DEBUGGER;

    # insert typeahead values
    _TYPE_AHEAD_( @_ );

    # execute break
    no warnings 'once';
    $DB::single = 1;
}

sub _DEBUGGER_ {
    # description: interrupt execution to execute debugger commands,
    # then continue execution

    # do nothing unless running in the debugger
    return unless IN_DEBUGGER;

    # do nothing if no debugger command supplied
    return unless @_;

    # execute a break point, perform the actions, then continue
    _BREAK_HERE_( @_, 'c' );
}

sub _TYPE_AHEAD_ {
    # description: insert debugger commands into the typeahead buffer that will be executed when the debugger returns to its REPL loop

    # do nothing unless running in the debugger
    return unless IN_DEBUGGER;

    no warnings 'once';
    push @DB::typeahead, @_;
}

__PACKAGE__;
