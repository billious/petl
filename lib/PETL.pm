package PETL;
use 5.10.0;
use strict;
use warnings;
no warnings qw(uninitialized);

use Class::Multimethods qw	(multimethod)	;
use ColumnIndex					;
use DBI                         ()              ;
use Exporter            qw      (import)        ;
use File::Spec                  ()              ;
use GenericViewer				;
use IO::File            qw	()		;
use List::Util          qw	()		;
use PETL::Connector     qw      (connector)     ;
use PETL::DBI           qw      ()              ;
use Publisher					;
use Symbol			()		;
use Sub::Name			()		;
use Time::HiRes			()		;


########################################
# PETL keywords
########################################
our @EXPORT_OK =
  qw(
      aa
      auto_aligned
      auto_named
      cache
      chomped
      choose
      cleave
      column
      columns
      db
      dbh
      define
      delimited
      every
      expose
      fields
      file
      filter
      glue
      go
      hol
      late
      loh
      lol
      long
      perform
      range
      regex
      show
      table
      union

      connector
      viewer
      publisher
   );
our %EXPORT_TAGS =
  (
   all => \@EXPORT_OK,
  );

  ##############################################################################
 ##
### HELPER packages so "my Code $cb = sub {}" won't throw exception
###
################################################################################
##
#
{
  # temporarily suppress 'only used once' messages
  no warnings 'once';
  @Code::ISA      = 'CODE';
  @Array::ISA     = 'ARRAY';
  @Hash::ISA      = 'HASH';
  @Connector::ISA = 'CODE';
}
  
##############################################################################
# CONSTANTS
##############################################################################
use constant IN_DEBUGGER => DB->can('DB');
use constant FALSE       => 0;
use constant TRUE        => ! FALSE;

use constant NO_HANDLER  => sub {};

##############################################################################
# SUBROUTINES
##############################################################################
sub _BREAK_HERE_;
sub auto_named;
sub publisher;
# sub viewer;
sub _timeit;

########################################
# AUTO_ALIGNED keyword
########################################
multimethod auto_aligned => qw( Publisher ) => sub {
    # accept publisher and return publisher that automatically aligns
    # columns by width
    my ($pub) = @_;

    return publisher
        ( sub {
              my GenericViewer $viewer = shift;

              my @widths;
              my $res = cache( $pub );

              # get the column widths
              my $scanner = sub {
                  my ($ig, $cells) = @_;
                  @widths = (0) x @$cells 
                      unless @widths;
                  for my $idx ( 0 .. $#$cells ) {
                      $widths[$idx] = List::Util::max
                          ( $widths[$idx], length( $cells->[$idx] ) );
                  }
              };
              $res->( viewer( on_header => $scanner, on_row => $scanner ) );

              # create the sprintf format string
              my $format = join ' ', map( "\%-${_}s", @widths ) ;

              # how table is constructed with the format
              my $row_count = 0;
              my $out = publisher
                  ( publisher => $res,
                    on_header => sub {
                        my ($v, $header) = @_;
                        # format column names
                        $v->add_header([ sprintf( $format, @$header ) ]);

                        # make a formatted seperator row as first row
                        my @under = map '='x$_, @widths;
                        $v->add_row([ sprintf( $format, @under ) ]);
                    },
                    on_row    => sub {
                        my ($v, $row) = @_;
                        $row_count++;
                        $v->add_row([ sprintf( $format, @$row ) ]);
                    }, 
                    on_end   => sub {
                        my GenericViewer $v = shift;
              
                        $v->add_row([ $_ ]) for ( '', "Rows received: $row_count" );
                    },
                );

              # transfer table from cache through formatter to topmost viewer
              $out->( $viewer );
          }
      );
};

# aa is a synonym for auto_aligned
sub aa { &auto_aligned(@_) }

########################################
# CACHE keyword
########################################
# cache results from publisher
multimethod cache => ('Publisher') => sub {
  # first invocation of returned Publisher
  my Publisher $pub = shift;

  my $lol;
  return publisher
    (sub {
       my GenericViewer $viewer = shift;

       # create lol on first invocation
       $lol ||= lol( $pub );

       # use table to convert lol back to a Publisher, and transfer it
       # to the downstream viewer
       table( $lol )->( $viewer );
     });
};

########################################
# CHOMPED keyword
########################################
multimethod chomped => qw(Publisher) => sub {
  # perform 'chomp' on the first column of each row
  my Publisher $pub = shift;

  return publisher
    (
     publisher => $pub,
     on_row    => sub {
       my GenericViewer $v   = shift;
       my               $row = shift;

       chomp $row->[0];
       $v->add_row([ $row->[0] ]);
     },
    );
};

########################################
# CHOOSE keyword
########################################
# choose columns from publisher
multimethod choose => qw(ARRAY Publisher) => sub {
  my $ra_columns	= shift;
  my Publisher $pub	= shift;

  # use a new ColumnIndex object to generate a callback to retrieve
  # the requested columns or throw exception. 
  my ColumnIndex $cm = ColumnIndex->new();
  my $cb = $cm->get_cb( @$ra_columns );

  return publisher
    (
     publisher => $pub,
     on_header => sub {
       my ($v, $header) = @_;
       $cm->add_header( $header );
       $v->add_header([ $cb->($header) ]);
     },
     on_row    => sub {
       my ($v, $row) = @_;
       $v->add_row([ $cb->($row) ]);
     },
    );
};

# choose columns defined in a whitespace delimited string
multimethod choose => qw($ Publisher) => sub {
  my ($columns_string, $pub) = @_;
  return choose( [split ' ', $columns_string], $pub );
};


########################################
# COLUMN keyword
########################################
# column with a name and list reference is a one column table
multimethod column => qw($ ARRAY) => sub {
  my ($column_name, $ra_values) = @_;
  return publisher
    (
     on_start => sub {
       my ($v) = @_;
       $v->add_header([ $column_name ]);
       for my $value ( @$ra_values ) {
	 $v->add_row([ $value ]);
       }
     },
    );
};

########################################
# COLUMNS keyword
########################################
# choose column(s) by index instead of 
# name
multimethod columns => qw(ARRAY Publisher) => sub {
    my Array     $indexes = shift;
    my Publisher $pub     = shift;

    # return the publisher
    return publisher
        (
            publisher => $pub,

            # generate handlers for array splicing
            map {
                my $event_name = $_;

                'on_' . $event_name => sub {
                    my GenericViewer $v     = shift;
                    my Array         $cells = shift;

                    my $method_name = 'add_' . $event_name;
                    $v->$method_name([ @{$cells}[ @$indexes ] ]);
                }
            } qw( header row ),
        );
};

########################################
# DB keyword: synonym for dbh
########################################
sub db { &dbh }

########################################
# DBH keyword
########################################
# dbh query using static query
multimethod dbh => qw(DBI::db $) => sub {
  my DBI::db $dbh = shift;
  my         $sql = shift;

  return publisher
    (sub {
       my GenericViewer $viewer = shift;

       my DBI::st $sth = $dbh->prepare( $sql );
       my $rows = $sth->execute();

       # transfer
       PETL::DBI::statement( $sth )->( $viewer );
     });
};

# dbh dbh, query using dynamic query
multimethod dbh => qw(DBI::db CODE) => sub {
  my ($dbh, $dyna) = @_;

  return publisher
      ( sub {
            my GenericViewer $viewer = shift;
            dbh( $dbh, &$dyna() )->( $viewer );
        });
};

# dbh callback, dynamic query
multimethod dbh => qw(PETL::Connector CODE) => sub {
    #_BREAK_HERE_;
    my PETL::Connector  $get_dbh_cb = shift;
    my Code             $dyna       = shift;

    return publisher( sub {
        my GenericViewer $v = shift;
        dbh( &$get_dbh_cb, &$dyna )->( $v );
    });
};

# dbh query using static query and arguments
multimethod dbh => qw( DBI::db  $ ARRAY ) => sub {
  # use database handle as inner part of cartesian product with argument names
  my DBI::db $dbh		= shift;
  my         $sql		= shift;
  my Array   $ra_arg_names	= shift;

  my @arg_names = @$ra_arg_names;
  my $num_args = scalar @arg_names;
  return publisher( sub {
      my GenericViewer $viewer = shift;

      # throw exception if default hash doesn't have columns requested in the array
      my @missing = grep !exists $_{$_}, @arg_names;
      die "Requested columns ( @missing ) aren't present in outer context.\n"
	  if @missing;

      my DBI::st $sth  = $dbh->prepare( $sql );
      my         $rows = $sth->execute( @_{@arg_names} );
      return unless $rows;

      # transfer statement to viewer
      PETL::DBI::statement( $sth )->( $viewer );
  });
};

# dbh CODE, query, ARRAY allows db connectino to be late binding and
# used as inner part of cartesian product with argument names
multimethod dbh => qw( PETL::Connector $ ARRAY ) => sub {
    my PETL::Connector $connector = shift;
    my                 $sql       = shift;
    my Array           $fields    = shift;

    return publisher sub {
        my GenericViewer $v = shift;
        dbh( &$connector, $sql, $fields )->( $v );
    };
};

# dbh CODE, query allows db connection to be late binding
multimethod dbh => qw( PETL::Connector $ ) => sub {
    my PETL::Connector $get_dbh_cb = shift;
    my           $sql        = shift;

    return publisher sub {
        my GenericViewer $v = shift;
        dbh( &$get_dbh_cb(), $sql )->( $v );
    };
};

# dbh CODE, query, Publisher allows db connection to be late binding
multimethod dbh => qw( PETL::Connector $ Publisher ) => sub {
    my PETL::Connector $get_dbh_cb = shift; 
    my           $sql        = shift; 
    my Publisher $pub        = shift; 

    return publisher sub {
        my GenericViewer $v = shift;
        dbh( &$get_dbh_cb(), $sql, $pub )->( $v );
    };
};

# dbh query, usually insert or update, returns publisher that shows
# number of records returned from the execute
multimethod dbh => qw( DBI::db $ Publisher ) => sub {
  my ( $dbh, $sql, $pub ) = @_;

  # determine if query is from a SELECT statement and prepare the
  # query
  my $is_select = $sql =~ /^\s*SELECT\b/im;
  my $sth = $dbh->prepare( $sql );

  # flag to indicate whether we've sent the header or not
  my $has_header = FALSE;
  
  # how to behave if SELECT statement
  my $first;
  my $select = publisher
      (
          publisher => $pub,
          on_header => NO_HANDLER,
          on_row    => sub {
              my GenericViewer $v     = shift;
              my               $cells = shift;

              $first ||= _first( $v );
              $sth->execute( @$cells );
              PETL::DBI::statement( $sth )->( $first );
          },
          on_end    => sub {
              $first = undef;
          },
      );

  # how to behave IF NOT a SELECT statment
  my $not_select = publisher
      (
          publisher => $pub,
          on_header => NO_HANDLER,
          on_row    => sub {
              my GenericViewer $v     = shift;
              my               $cells = shift;

              $first ||= _first( $v );
              my $num_rows = $sth->execute( @$cells );

              $first->add_header([ 'rows'    ]);
              $first->add_row   ([ $num_rows ]);
          },
          on_end     => sub {
              $first = undef;
          },
      );
            
  # return the correct publisher
  return $is_select? $select: $not_select;
};

########################################
# 'DEFINE' keyword
########################################
# define act => 'dbh', $dbh;
sub define {
  # install a new subroutine in the caller's namespace that invokes
  # the named subroutine with the arguments supplied as well as
  # arguments that are supplied when the new subroutine is invoked
  my ($pkg)		= caller;
  my $new_keyword	= shift;
  my $func_name		= shift;
  my @define_args	= @_;

  # if $new_keyword is fully qualified subroutine name then the string
  # preceeding the last '::' is taken to be the pkg
  if ( my ( $full_pkg, $keyword ) = $new_keyword =~ /^(.*)::(.*)/ ) {
      ($pkg, $new_keyword) = ($full_pkg, $keyword);
  }

  # throw exception if the  new_keyword is already defined
  die "Can't define an pre-existing definition for '$new_keyword'\n"
    if $pkg->can( $new_keyword );

  # ensure that the function or subroutine referenced by the
  # definition exists or throw exception
  my $func;
  die "Unable to find function '$func_name'.  Definition of '$new_keyword' failed.\n"
    unless ($func) = grep defined, 
      map( $_->can( $func_name ), $pkg, __PACKAGE__ );

  # insert subroutine declaration into caller's namespace so
  # assignment below works correctly
  my $sym = Symbol::qualify_to_ref( $new_keyword, $pkg );

  # save any existing references in the glob slots
  my $hr_saver = {};
  for my $slot (qw(SCALAR ARRAY HASH)) {
    # ignore undefined scalars
    next if $slot eq 'SCALAR' && !defined ${*$sym{$slot}};

    # ignore undefined slots
    next unless defined *$sym{$slot};

    # save defined slots
    $hr_saver->{$slot} = *$sym{$slot};
  }

  # establish the slot holder so prototypes work
  *$sym = \&$sym;

  # assign the correct code ref to the newly defined keyword
  *$sym = sub {
    &$func( @define_args, @_ );
  };

  # restore pre-existing slots
  while ( my ($slot, $ref) = each %$hr_saver ) {
    given ( $slot ) {
      when ( 'SCALAR' ) { ${*$sym{$slot}} = $$ref }
      when ( 'ARRAY'  ) { @{*$sym{$slot}} = @$ref }
      when ( 'HASH'   ) { %{*$sym{$slot}} = %$ref }
    }
  }

  # clear the references so garbage collection works
  my @keys = keys %$hr_saver;
  @{$hr_saver}{@keys} = (undef) x @keys;

  # clear the saver hash
  %$hr_saver = ();
}

########################################
# CLEAVE keyword
########################################
multimethod cleave => qw($ Publisher) => sub {
  # Accepts delimiter and a Publisher.  Returns a publisher that splits
  # the first column of the input publisher by the delimiter
  my $delimiter      = shift;
  my Publisher $pub  = shift;

  return cleave( qr/$delimiter/, $pub );
};

multimethod cleave => qw(Regexp Publisher) => sub {
    my Regexp    $regex = shift;
    my Publisher $pub   = shift;


    return auto_named( publisher
        ( 
            publisher => $pub,
            on_header => NO_HANDLER,
            on_row    => sub {
                my GenericViewer $v   = shift;
                my               $row = shift;
                        
                $v->add_row([ split /$regex/, $row->[0] ]);
            },
        )
    );
};


multimethod auto_named => qw(GenericViewer) => sub {
    # accept a viewer, returns a viewer that will automatically name
    # the fields using the pattern 'fields%03d' if add_row is invoked
    # before add_header
    my GenericViewer $v = shift;

    my $has_header = FALSE;
    my $count      = 0;
    return viewer 
        (
            on_header => sub {
                my GenericViewer $ig     = shift;
                my Array         $header = shift;
                
                return if $has_header;
                $has_header = TRUE;
                $v->add_header( $header );
            },
            on_row    => sub {
                my GenericViewer $ig     = shift;
                my Array         $cells  = shift;

                unless( $has_header ) {
                    $has_header = TRUE;
                    $v->add_header([ map { sprintf 'field%03d', $count++  } @$cells ]);
                }
                $v->add_row( $cells );
            },
        );
};

multimethod auto_named => qw(Publisher) => sub {
    # Accept a publisher, returns a publisher that will automatically
    # name the fields using the pattern 'field%03d' if add_row is
    # invoked before add_header
    my Publisher $pub = shift;

    my $has_header = FALSE;
    return publisher sub {
        my GenericViewer $v = shift;
        $pub->( auto_named( $v ) );
    };
};

##############################################################################
# DELIMITED keyword
##############################################################################
multimethod delimited => qw($ $ Publisher) => sub {
  # Accept a column delimiter, a row delimiter, and a Publisher.
  # Returns publisher that combines columns and rows into desired
  # format
  my $column_delimiter = shift;
  my $row_delimiter    = shift;
  my Publisher $pub    = shift;

  my $num_cols;
  my Code $reset_num_cols = sub {
      $num_cols = undef;
  };
  my Code $formatter = sub {
    my Array $row = shift;
    $num_cols ||= scalar @$row;
    return join( $column_delimiter, @{$row}[ 0 .. $num_cols-1 ] ) . $row_delimiter;
  };

  return publisher
    (
     publisher => $pub,
     on_start  => $reset_num_cols,
     on_end    => $reset_num_cols,
     on_header => sub {
       my GenericViewer $v   = shift;
       my Array         $row = shift;
       $v->add_header([ $formatter->( $row ) ]);
     },
     on_row    => sub {
       my GenericViewer $v   = shift;
       my Array         $row = shift;
       $v->add_row   ([ $formatter->( $row ) ]);
     },
    );
};

multimethod delimited => qw( $ Publisher ) => sub {
    # Accepts column delimiter and a publisher
    # returns a publisher that assumes the row delimiter is "\n"
    my           $column_delimiter = shift;
    my Publisher $pub              = shift;

    return delimited( $column_delimiter, "\n", $pub );
};

########################################
# EVERY keyword
########################################
# every loop for two publishers
multimethod every => qw(Publisher Publisher) => sub {
  my ($outside_pub, $inside_pub) = @_;

  return publisher sub {
    my ($viewer) = @_;

    my (@fields, @values, @stack, @inner_header, $had_rows);

    my $inner = viewer
      (
       on_header => sub {
	 my ($ig, $inner_header) = @_;
         return if @inner_header;

         @inner_header = @$inner_header;
         $viewer->add_header([ @fields, @inner_header ]);
       },
       on_row    => sub {
	 my ($ig, $inner_row) = @_;

	 $had_rows++;

	 if( @stack ) {
	   # stack is not empty so there must be some outer rows that
	   # are awaiting output - send them out
	   for my $row ( @stack ) {
	     $viewer->add_row
	       ([ @values[0 .. $#fields],
		  @$inner_row[0 .. $#inner_header]
		]);
	   }
	   @stack = ();
	 }

	 $viewer->add_row
	   ([ @values[0 .. $#fields],
	      @$inner_row[0 .. $#inner_header]
	    ]);
       },
      );

    my $outer = viewer
      (
       on_header => sub {
	 my ($ig, $outer_header) = @_;
	 @fields = @$outer_header;
       },
       on_row    => sub {
	 my ($ig, $outer_row) = @_;

	 local %_;
	 @_{@fields} = @values = @$outer_row;

	 # clear the had rows flag and run the inner publisher
	 $had_rows = 0;
	 $inside_pub->( $inner );

	 if( @inner_header ) {
	   return if $had_rows;

	   # had inner header but no rows returned, output undefs for
	   # inner columns
	   $viewer->add_row([ @values[0 .. $#fields],
			      (undef) x @inner_header ]);
	 }
	 else {
	   # if the inner publisher didn't return results then stack
	   # our results
	   push @stack, $outer_row;
	 }
       },
      );

    # execute the outer publisher
    $outside_pub->( $outer );

    # if there still is a stack then the inside publisher never sent a
    # row, output the stack with the outer headers
    return unless @stack;
    $viewer->add_header( \@fields );
    for my $row ( @stack ) {
      $viewer->add_row([ @$row[0 .. $#fields] ]);
    }
  };
};

# every with an ARRAY allows arbitrary nesting of publishers
multimethod every => qw(ARRAY) => sub {
  my ($ra_pubs) = @_;
  my @pubs = @$ra_pubs;

  die "FATAL: Must supply at least two publishers to every keyword.\n"
    unless @pubs > 1;

  my $chain;
  while( @pubs ) {
    if( $chain ) {
      # chain already defined, only add one link to existing chain
      $chain = every( pop(@pubs), cache( $chain ) );
    }
    else {
      # chain not defined, yank last two links off the list of pubs
      $chain = every( splice(@pubs, -2, 2) );
    }
  }

  return $chain;
};

########################################
# EXPOSE keyword
########################################
# accepts a publisher and uses symbol table manipulation to expose the
# columns as named lists in the caller's namespace
#
# eg:  expose table [[a..c],[1..3],[4..6]]
#
#     # will create three arrays in current package: @a, @b, @c
#     #   @a = (1,4)
#     #   @b = (2,5)
#     #   @c = (3,6)
multimethod expose => qw(Publisher) => sub {
  my ($pub) = @_;

  # discover the package of the caller.  We use offset of 1 since
  # we're called indirectly by the caller via Class::Multimethods
  my ($pkg) = caller(1);

  # get a hash of lists of data in $pub
  my $hol = hol( $pub );

  # inject the lists into the caller's package
  for my $fd ( keys %$hol ) {
    my $ra = *{Symbol::qualify_to_ref( $fd, $pkg )};
    @$ra = @{$hol->{$fd}};
  }
};

########################################
# FIELDS keyword
########################################
# fields called with ARRAY, Publisher returns Publisher with renamed
# columns
multimethod fields => qw(ARRAY Publisher) => sub {
  my ($fields, $pub) = @_;
  return publisher
    ( on_header => sub {
	my ($v, $header) = @_;
	$v->add_header([ @$fields ]);
      },
      on_row    => sub {
	my ($v, $row) = @_;
	$v->add_row([ @$row[0 .. $#$fields] ]);
      },
      publisher => $pub,
    );
};

# fields called with $, Publisher assumes scalar is space delimited
# string of fields
multimethod fields => qw($ Publisher) => sub {
  my ($fields_string, $pub) = @_;
  fields( [ split ' ', $fields_string ], $pub );
};

########################################
# FILE keyword
########################################
multimethod file => qw($ Publisher) => sub {
  # write contents of Publisher rows to filename
  my ($filename, $pub) = @_;

  my IO::File $handle;
  return publisher
    (
     # open output file for writing
     on_start => sub {
       die "Can't open file '$filename' for writing. $_"
	 unless $handle = IO::File->new( $filename, 'w' );
     },

     # delegate transfer of rows to file via file(IO::File, Publisher)
     publisher => sub {
       my GenericViewer $v = shift;
       file( $handle, $pub )->( $v );
     },

     # close file handle
     on_end   => sub {
       $handle = undef;
     },
    );
};

multimethod file => qw(IO::File Publisher) => sub {
  # write contents of first column of Publisher to filehandle
  my IO::File $output_handle;
  my Publisher $pub;
  ($output_handle, $pub) = @_;

  my $num_rows;
  return publisher sub {
      my GenericViewer $summary = shift;

      # initialize $num_rows to zero
      $num_rows = 0;

      # send first column of each row to output handle, increment row
      # count
      $pub->( 
          viewer (
              on_row => sub {
                  my ($ignore, $row) = @_;
                  $output_handle->print( $row->[0] );
                  $num_rows++;
              },
          )
      );

      # write report of number of rows seen
      column( 'rows', [$num_rows] )->( $summary );
  }
};

multimethod file => qw(CODE Publisher) => sub {
    # file called with callback invokes the callback and delegates back to 'file' command
    my Code      $cb  = shift;
    my Publisher $pub = shift;

    return publisher sub {
        file( &$cb, $pub )->( @_ );
    };
};

# file called with a code reference invokes the code reference and delegates to FILE
multimethod file => qw(CODE) => sub {
    my Code $cb = shift;

    return publisher sub {
        file( &$cb )->( @_ );
    };
};

# file called with a string assumes string is a filename to be read
multimethod file => qw($) => sub {
  my ($filename) = @_;

  return publisher sub {
      my GenericViewer $v = shift;

      # open the file to be read
      my IO::File $input_handle = IO::File->new( $filename, 'r' )
	  or die "Can't open file '$filename' for reading.\n";

      # delegate reaading to file( IO::File ) and use filename as column name
      fields( [$filename], file( $input_handle ) )->( $v );
  };
};

# file called with a IO::File assumes input filehandle
multimethod file => qw(IO::File) => sub {
    my IO::File $input_handle = shift;

    return publisher sub {
        my GenericViewer $v = shift;

        # output a constructed column name
        $v->add_header([ sprintf( "IO::File#%05d", $input_handle->fileno ) ]);

        # output the rows in a single column
        while ( my $line = <$input_handle> ) {
            $v->add_row([ $line ]);
        }
    };
};

########################################
# FILTER keyword
########################################
# act like perl's grep: accept a code ref and a publisher, return
# publisher that only sends rows that generate 'true' value from the
# coderef
multimethod filter => qw(CODE Publisher) => sub {
  my Code $predicate = shift;
  my Publisher $pub  = shift;

  return publisher
    (
     publisher => $pub,
     on_row    => sub {
       my GenericViewer $v   = shift;
       my Array         $row = shift;

       $v->add_row( $row )
	 if $predicate->([ @$row ]);
     },
    );
};

########################################
# GO keyword
########################################
# similar to show/perform except only number of rows is returned
multimethod go => qw(Publisher) => sub {
    my Publisher $input = shift;

    # create viewer to count rows
    my $rows = 0;
    my GenericViewer $counter = viewer
        (
            on_row => sub {
                $rows++;
            },
            on_header => NO_HANDLER,
        );

    my $dur = _timeit( sub { $input->( $counter ) } );

    STDOUT->say   ( 'Rows    : '      , $rows );
    STDOUT->printf( "Duration: %.3f\n", $dur  );
};

########################################
# GLUE keyword
########################################
# accepts multiple publishers and glue their columns together
sub glue {
    my (@pubs) = @_;

    return publisher
        (
            on_start => sub {
                my ($viewer) = @_;

                # place publisher's output into an array of lols
                my @lols = map lol($_), @pubs;
       
                # output composite header
                $viewer->add_header([ map @{$_->[0]}, @lols ]);

                # output all the rows
                my $num_rows = List::Util::max( map( $#$_, @lols ) );
                for my $row ( 1 .. $num_rows ) {
                    $viewer->add_row([ map @{$_->[$row]||[]}[0..$#{$_->[0]}], @lols ]);
                }
            },
        );
}

########################################
# HOL keyword
########################################
# accepts a publisher and returns a hash reference of lists of data
#
# eg: $h_of_l = hol table [[a..b],[1..2],[3..4]]
#
#  $h_of_l =  {
#               a => [1,3],
#               b => [2,4],
#             }
multimethod hol => qw(Publisher) => sub {
    my ($pub) = @_;

    # create container for result
    my $hol = {};

    my @fields;
    $pub->
        ( 
            viewer( 
                on_header => sub {
                    my ($ig, $header) = @_;

                    @fields = map {
                        my $cp = $_;
                        $cp =~ s/[^a-z0-9]/_/gi;
                        $cp
                    } @$header;

                    # create the slots
                    @${hol}{@fields} = map [], @fields;
                },
                on_row    => sub {
                    my ($ig, $row) = @_;
                    
                    # push new value onto list in each slot
                    for my $idx ( 0 .. $#fields ) {
                        push @{$hol->{$fields[$idx]}}, $row->[$idx];
                    }
                },
            )
        );

    # return results
    return $hol;
};

# hol keyword given a hash assumes a hash of lists 
multimethod hol => qw(HASH) => sub {
    my Hash $hol = shift;

    return publisher sub {
        my GenericViewer $v = shift;

        # do nothing unless there is at least one key
        return unless keys %$hol;

        # output header
        $v->add_header([ 'key', 'entry' ]);
        
        while ( my ($key, $listref) = each %$hol ) {
            next unless defined $listref;
            next unless @{ $listref || [] };
            for my $entry ( @$listref ) {
                $v->add_row([ $key, $entry ]);
            }
        }
    };
};

########################################
# LATE keyword
########################################
# late called with string returns code ref that evals the string
multimethod late => qw($) => sub {
  my ($dynamic_string) = @_;
  return sub {
    local $@;
    my $result = eval qq("$dynamic_string");
    if( $@ ) {
      # rethrow errors
      die $@;
    }
    return $result;
  };
};

########################################
# LOH keyword
########################################
# loh called with an ARRAY returns a publisher over contents
multimethod loh => ('ARRAY') => sub {
  my ($loh) = @_;
  return publisher sub {
      my GenericViewer $viewer = shift;

      #- scan loh for distinct keys
      my ($keys);
      @{$keys}{map( keys(%$_), @$loh)} = ();
      my @fields = keys %$keys;
      $viewer->add_header( \@fields );

      #- replay rows using the same pattern of columns
      for my $row_hash ( @$loh ) {
          $viewer->add_row([ @{$row_hash}{@fields} ]);
      }
  };
};

# loh called with a Publisher returns a loh of the contents
multimethod loh => ('Publisher') => sub {
  my ($pub) = @_;
  my $loh;

  # have publisher write its contents into a viewer
  my @fields;
  $pub->( viewer
              (
                  on_header => sub {
                      my ($ig, $header) = @_;
                      @fields = @$header;
                  },
                  on_row    => sub {
                      my ($ig, $row) = @_;
                      my $href;
                      @{$href}{@fields} = @$row;
                      push @$loh, $href;
                  },
              )
          );

  return $loh;
};

########################################
# LOL keyword
########################################
# lol called with a publisher returns the contents of the publisher in
# a list of lists
multimethod lol => ('Publisher') => sub {
  my ($pub) = @_;

  my $lol;
  my $common = sub {
    my ($ig, $cells) = @_;
    push @$lol, [ @$cells ];
  };
  $pub->( viewer( on_header => $common, on_row    => $common, ) );
  return $lol;
};

# lol called with an ARRAY is the same as the 'table' function
multimethod lol => ('ARRAY') => sub {
  my ($list_of_lists) = @_;
  return table( $list_of_lists );
};

########################################
# LONG keyword
########################################
# 'long' format shows one column per row and rows seperated by blank line
multimethod long => ('Publisher') => sub {
  my Publisher $pub = shift;

  my $format;
  my @header;
  return publisher
    (
     publisher => $pub,
     on_header => sub {
       my ($v, $header) = @_;
       @header = @$header;

       $format = '%' . List::Util::max( map length, @header ) . 's: %s';
       $v->add_header([ 'result' ]);
     },
     on_row    => sub {
       my ($v, $row   ) = @_;

       for my $column_index ( 0 .. $#header ) {
	 $v->add_row([ sprintf( $format, $header[$column_index], $row->[$column_index] ) ]);
       }
       $v->add_row([ '' ]);
     },
    );
};

########################################
# PERFORM keyword
########################################
# 'perform' function
multimethod perform => ('Publisher') => sub {
  my ($pub) = @_;

  my Code $action = sub {
      eval {
          $pub->( _tabd() );
      };
      if( $@ ) {
          STDERR->print( $@ );
      }
  };

  my $dur = _timeit $action;

  STDOUT->say( sprintf( "Duration: %.3f seconds.", $dur ) );
};

########################################
# PUBLISHER keyword
########################################
multimethod publisher => qw( CODE ) => sub {
    # publisher with a code reference returns a new Publisher::Simple
    my Code $cb = shift;
    return Publisher::Simple->new( $cb );
};

resolve_no_match publisher => sub {
    # publisher with anything else is a synonym for _pub( @_ )
    return Publisher->new( @_ );
};

########################################
# RANGE keyword
########################################
# accept two numbers and publish integer values between them (like 'for')
multimethod range => ('#', '#') => sub {
  my ($from, $to) = @_;
  return publisher 
    ( on_start => sub {
	my ($v) = @_;
	$v->add_header([ 'range' ]);
	for my $n ( $from .. $to ) {
	  $v->add_row([ $n ]);
	}
      }
    );
};

########################################
# REGEX keyword
########################################
# accept a regex and a publisher, return publisher of captures found
# by regex in first column of input publisher
multimethod regex => qw(Regexp Publisher) => sub {
  my Regexp    $regex = shift;
  my Publisher $pub   = shift;

  my Code $cb;
  my $has_header = 0;
  return auto_named publisher
    (
     publisher => $pub,
     on_header => sub {
     },
     on_row    => sub {
       my GenericViewer $v      = shift;
       my Array         $row    = shift;

       # only output a row if there is a match
       my @matched = $row->[0] =~ /$regex/;
       return unless @matched > 0;


       # send the captured sells as a row
       $v->add_row( \@matched );
     },
    );
};

# regex with string and publisher
multimethod regex => qw($ Publisher) => sub {
  my $regexp        = shift;
  my Publisher $pub = shift;
  return regex( qr/$regexp/, $pub );
};

########################################
# SHOW keyword
########################################
# show is a synonym for perform
sub show { perform(@_) }

########################################
# TABLE keyword
########################################
# table for an ARRAY
multimethod table => ('ARRAY') => sub {
  # accept a list of lists and return a Publisher over those values
  my ($lol) = @_;

  return publisher
    (
     sub {
       my ($viewer) = @_;

       # do nothing if array is empty
       return unless @$lol;

       # first index is the headers
       my @header = @{$lol->[0]};
       $viewer->add_header( \@header );

       # remaining indices are the rows
       for my $row ( 1 .. $#$lol ) {
	 $viewer->add_row([ @{$lol->[$row]}[ 0.. $#header ] ]);
       }
     }
    );
};

sub one_header;
multimethod one_header => qw(Publisher) => sub {
    my Publisher $pub = shift;

    my $has_header = FALSE;
    return publisher
        (
            publisher => $pub,
            on_start  => sub {
                $has_header = FALSE;
            },
            on_header => sub {
                my GenericViewer $v     = shift;
                my               $cells = shift;
                return if $has_header;
                $has_header = TRUE;
                $v->add_header( $cells );
            },
            on_row    => sub {
                my GenericViewer $v     = shift;
                my               $cells = shift;
                $v->add_row   ( $cells );
            },
        );
};    

sub same_columns;
multimethod same_columns => qw(Publisher) => sub {
    my Publisher $pub = shift;

    my $num_cols;
    return publisher
        (
            publisher => $pub,
            on_start  => sub {
                $num_cols = undef;
            },
            on_header => sub {
                my GenericViewer $v     = shift;
                my               $cells = shift;
                $num_cols = scalar @$cells;
                $v->add_header( $cells );
            },
            on_row    => sub {
                my GenericViewer $v     = shift;
                my               $cells = shift;
                $num_cols ||= scalar @$cells;
                $v->add_row([ @{$cells}[ 0 .. $num_cols-1 ] ]);
            },
        );
};

########################################
# UNION keyword
########################################
# serially combine output from one to many publishers
sub union {
    my @publishers = @_;

    # send the output of each publisher in @publishers to viewer $v,
    # ensuring that only one header and a consistent number of columns
    # is sent
    return same_columns one_header publisher sub {
        my GenericViewer $v = shift;

        for my Publisher $p ( @publishers ) {
            $p->( $v );
        }
    };
};

########################################
# VIEWER keyword
########################################
# syntactic sugar to return viewer
sub viewer { GenericViewer->new( @_ ) }

##############################################################################
# PRIVATE FUNCTIONS
##############################################################################
sub _appender {
    # Accept:
    # -   an array of headers for left-most cells
    # -   a callback to retrieve the row values for those cells
    # -   a downstream viewer.  
    #
    # Returns a viewer
    # -  that places the left header cells before the header received from
    # the publisher, and the left cells before the cells received from
    # the publisher
    my Array         $left_headers  = shift;
    my Code          $left_cells_cb = shift;
    my GenericViewer $viewer        = shift;

    my $has_header = FALSE;

    return viewer
        (
            on_header => sub {
                my ($ig, $header) = @_;
                $viewer->add_header([ @$left_headers, @$header ])
                    unless( $has_header||=TRUE );
            },
            on_row    => sub {
                my ($ig, $row   ) = @_;
                $viewer->add_row   ([ @{ &$left_cells_cb || [] }  , @$row    ]);
            },
        );
}

sub _first {
    # accepts viewer,  returns viewer that only propagates first header received
    my GenericViewer $original = shift;

    my $has_header = FALSE;
    return viewer
        (
            on_header => sub {
                my GenericViewer $self  = shift;
                my               $cells = shift;

                return if $has_header++;
                $original->add_header([ @$cells ]);
            },
            on_row    => sub {
                my GenericViewer $self  = shift;
                my               $cells = shift;

                $original->add_row([ @$cells ]);
            },
        );
}

sub _tabd {
  # return a viewer that outputs tab delimited

  my @columns;
  return viewer
    (
     on_header => sub {
       my ($ig, $header) = @_;
       @columns = @$header;
       STDOUT->say( join(qq(\t), @$header) );
     },
     on_row    => sub {
       my ($ig, $row) = @_;
       STDOUT->say( join(qq(\t), @$row[0..$#columns]) );
     },
    );
}

sub _timeit {
    # return the number of seconds taken to perform the callback
    my Code $cb = shift;

    my $before = Time::HiRes::time();
    $cb->();
    my $after  = Time::HiRes::time();

    return $after - $before;
}

sub _BREAK_HERE_ { 
  # allow persistent debugger commands and breaks
  return unless IN_DEBUGGER;

  no warnings 'once';
  push @DB::typeahead, @_;
  $DB::single = 1 ;
}

no Exporter;
return __PACKAGE__;


__END__

=head1 NAME

PETL

=head1 DESCRIPTION

Perl subroutine library for Extract, Transformation, and Load (ETL)

=head1 SYNOPSIS

  use strict;
  use PETL ':all';

  # define $a as a static table of 2 rows and three columns: a, b, and c
  my $a = table [
	       [ 'a' .. 'c' ],
               [  1  ..  3  ],
               [  4  ..  6  ],
              ];

  # display the table
  show $a;

  # display the table in reverse column order
  show choose [ 'c', 'b', 'a' ], $a;

  # rename the columns 'me', 'myself', and 'I'
  show fields [ qw(me myself I) ], $a;

  # another way to rename the columns 'me', 'myself', and 'I'
  show fields 'me myself I', $a;

  # aa qualfier auto formats presentation
  show aa $a;

  # make SQL queries against DBI instance
  my $dbh = DBI->connect( $dsn, $user, $pass );
  show aa dbh $dbh, 'SELECT table_name, column_name FROM INFORMATION_SCHEMA.columns';

  # perform inserts to SQL
  perform dbh $dbh, 'INSERT INTO three_columned_table VALUES (?, ?, ?)', $a;

  # define and use descriptive keyword of database connection
  define mydb => 'dbh', $dbh;
  show aa mydb 'SELECT * FROM table';
  perform mydb 'INSERT INTO some_table VALUES(?, ?, ?)', $a;

  # write table to tab-seperated-values file 'foo.tsv'
  perform file 'foo.tsv', delimited "\t", "\n", $a;

  # display first two columns of tab-seperated-values file 'foo.tsv'
  show fields '1 2', delimited "\t", chomped file 'foo.tsv';

  # convert a table into a List of Lists
  my $LoL = lol $a;
  my @column_names = @{ shift @$LoL };

  # cartesian product of two tables (flat for loop)
  my $binary = column 'binary', [ 0 .. 1 ];
  show aa every [ fields( '2', $binary ), fields( '1', $binary ) ];

=cut

