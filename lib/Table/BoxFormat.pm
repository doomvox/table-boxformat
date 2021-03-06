package Table::BoxFormat;
use Moo;
use MooX::Types::MooseLike::Base qw(:all);

=head1 NAME

Table::BoxFormat - Parsing the tabular data format generated by database SELECTs

=head1 VERSION

Version 0.03

=cut

our $VERSION = '0.03';
my  $DEBUG   = 0;   
use 5.10.0;  # time to start saying 'say'
use utf8::all;
use Carp;
use Data::Dumper;
use Table::BoxFormat::Unicode::CharClasses ':all'; # IsHor IsCross IsDelim

use Text::CSV; 

=encoding utf8

=head1 SYNOPSIS

   use Table::BoxFormat;
   # Reading input from a "dbox" temp file
   my $dbx = Table::BoxFormat->new( input_file => '/tmp/select_result.dbox' );
   my $data = $self->data;  # array of arrays, header in first row

   # Input dbox from a string
   my $dbx = Table::BoxFormat->new( input_data => $dboxes_string );
   my $data = $self->data;  # array of arrays, header in first row

   # input from dbox file, output directly to a tsv file
   my $dbx = Table::BoxFormat->new();
   $dbx->output_to_tsv( '/tmp/select_result.dbox', '/tmp/select_result.tsv' );

   # input dbox from a string, output directly to a tsv file
   $dbx = Table::BoxFormat->new( input_data => $dbox_string );
   $dbx->output_to_tsv( $output_tsv_file );


=head1 DESCRIPTION

Table::BoxFormat is a module to work with data in the tabular text
format(s) commonly used in database client shells (postgresql's
"psql", mysql's "mysql", or sqlite's "sqlite3"),
where a SELECT will typical display data in a form such as this (mysql):

  +-----+------------+---------------+-------------+
  | id  | date       | type          | amount      |
  +-----+------------+---------------+-------------+
  |  11 | 2010-09-01 | factory       |   146035.00 |
  |  15 | 2011-01-01 | factory       |   191239.00 |
  |  16 | 2010-09-01 | marketing     |   467087.00 |
  |  17 | 2010-10-01 | marketing     |   409430.00 |
  +-----+------------+---------------+-------------+

Or this (postgresql's "ascii" form):

   id |    date    |   type    | amount
  ----+------------+-----------+--------
    1 | 2010-09-01 | factory   | 146035
    4 | 2011-01-01 | factory   | 191239
    6 | 2010-09-01 | marketing | 467087
    7 | 2010-10-01 | marketing | 409430

These formats are human-readable, but not suitable for other
purposes such as feeding to a graphics program, or inserting into
another database table.

This code presumes these text tables of "data boxes" are either
stored in a string or saved to a file.  When stored in a file, 
I suggest using the extension ".dbox".

This code works with at least three different
formats: mysql, psql and unicode psql.

=head2 implementation notes

The main method here is L<read_dbox>, which works by first
looking for a horizontal ruler line near the top of the data,
for example:

  +-----+------------+---------------+-------------+
  ----+------------+-----------+--------
  ────┼────────────┼───────────┼────────

These ruler lines are used to identify the boundary columns,
after which the header and data lines are treated as fixed-width
fields.  Leading and trailing whitespace are stripped from each
value.

An earlier (now deprecated) method named L<read_simple> takes an
opposite approach, ignoring the horizontal rules entirely and
doing regular expression matches looking for data delimiters on
each line.  In comparison, the L<read_dbox> should run faster and
be able to handle strings with delimiter characters embedded in
them.


=head1 METHODS

=over

=cut

=item new

Creates a new Table::BoxFormat object.

Takes a list of attribute/setting pairs as an argument.

=over

=item input_encoding

Default's to "UTF-8".  Change to suit text encoding (e.g. "ISO-8859-1").
Must work as a perl ":encoding(...)" layer.

=item output_encoding

Like L<input_encoding>.  Default: "UTF-8".

=item input_file

File to input data from.  Can be supplied later, e.g. when
L<read_dbox> is called.  Only required if L<input_data> was
not defined directly.  (( TODO change this: make it required ? ))

=item input_data

SQL SELECT output in the fixed-width-plus-delimiter form discussed above.

=item the parsing regular expressions (type: RegexpRef)

=over

=item separator_rule

The column separators (vertical bar)

=item ruler_line_rule

Matches the Horizontal ruler lines (typically just under the
header line)

=item cross_rule

Match cross marks the horizontal bars typically use to mark
column boundaries.

=item left_edge_rule

Left border delimiters (we strip these before processing).

=item right_edge_rule

Right border delimiters (we strip these before processing).

=back

=back

=cut

# encodings default to utf-8 (might need to change, e.g. ISO-8859-1)
has input_encoding  => ( is => 'rw', isa => Str, default => 'UTF-8' );
has output_encoding => ( is => 'rw', isa => Str, default => 'UTF-8' );

# input file name (can skip if input_data is defined directly, or if file provided later)
has input_file  => ( is => 'rw', isa => Str, default => "" );

# can define input data directly, or alternately slurp it in from a file
#    TODO better to avoid slurping, work line-at-a-time?
has input_data  => ( is => 'rw', isa => Str,
                     default =>
                     sub { my $self = shift;
                           $self->slurp_input_data; },
                     lazy => 1 );

has data => ( is => 'rw', isa => ArrayRef,
              default =>
              sub { my $self = shift;
                    $self->read_dbox(); },
              lazy => 1 );

has header => ( is => 'rw', isa => ArrayRef, default => sub{ [] } );

has format => ( is => 'rw', isa => Str,    default => sub{ '' } );

# info about format/style of last data read
has meta   => ( is => 'rw', isa => HashRef,  default => sub{ {} } );

has separator_rule  => ( is => 'rw', isa => RegexpRef,
                       default =>
                       sub{ qr{
                                \s+         # require leading whitespace
                                \p{IsDelim}
                                    {1,1}   # just one
                                \s+         # require trailing whitespace
                            }xms } );

# horizontal dashes plus crosses or whitespace
has ruler_line_rule => ( is => 'rw', isa => RegexpRef,
                       default =>
                       sub{
                         qr{ ^
                             [ \p{IsHor} \s ] +
                             $
                            }x
                          }
                     );

has cross_rule  => ( is => 'rw', isa => RegexpRef,
                       default =>
                       sub{ qr{
                                \p{IsCross}
                                    {1,1}   # just one 
                            }xms } );

# To match table borders (e.g. mysql-style)
has left_edge_rule => ( is => 'rw', isa => RegexpRef,
                       default => sub{ qr{ ^ \s* [\|] }xms } );

has right_edge_rule => ( is => 'rw', isa => RegexpRef,
                       default => sub{ qr{ [\|] \s* $ }xms } );



=item slurp_input_data

Example usage:

  $self->slurp_input_data( $input_file_name );

=cut

sub slurp_input_data {
  my $self = shift;

  # the input file can be defined at the object level, or supplied as an argument
  # if it's an argument, the given value will be stored in the object level
  my $input_file;
  if ( $_[0] ) {
    $input_file = shift;
    $self->input_file( $input_file );
  } else {
    $input_file = $self->input_file;
  }

  croak "Need an input file to read a dbox from" unless( $input_file );
  my $input_encoding = $self->input_encoding;
  unless ( $input_file ) {
    croak
      "Needs either an input data file name ('input_file'), " .
      "or a multiline string ('input_data')  ";
  }
  my $in_enc = "<:encoding($input_encoding)";
  open my $fh, $in_enc, $input_file or croak "$!";
  local $/; # localized slurp mode
  my $data = <$fh>;

#   # strip leading trailing ws, including blank lines.
#   $data =~ s{ \A [\s]* }{}xms;
#   $data =~ s{    [\s]* \z}{}xms;

  return $data;
}




=item read_dbox

Given data in tabular boxes from a multiline string,
convert it into an array of arrays.

   my $data =
         $bxs->read_dbox();

Converts the boxdata from the object's input_data into an array
of arrays, with the field names included in the first row.

As a side-effect, copies the header (first row of returned data)
in the object's L<header>, and puts some format metadata in the object's L<meta>.

=cut

# Uses the header ruler cross locations to identify the column boundaries,
# then treats the data as fixed-width fields, to handle the case
# of strings with embedded separator characters.
sub read_dbox {
  my $self = shift;

  # the input file can be defined at the object level, or supplied as an argument
  # if it's an argument, the given value will be stored in the object level
  my $input_file;
  if( $_[0] ) {
    $input_file = shift;
    $self->input_file( $input_file );
  }

  my $input_data     = $self->input_data;
  my $ruler_line_rule = $self->ruler_line_rule;

  my $left_edge_rule  = $self->left_edge_rule;
  my $right_edge_rule = $self->right_edge_rule;

  my @lines = split /\n/, $input_data;

  # look for a header ruler line
  # (first or third line for mysql, second line for postgres),
  my (@pos, $format, $first_data, $header_loc, $ruler, @data);
 RULERSCAN:
  foreach my $i ( 1 .. 2 ) { # ruler lines are always near top
    my $line = $lines[ $i ];

    if( $line =~ m{ $ruler_line_rule }x ) { 
      ( $format, $header_loc, $first_data, @pos ) = $self->analyze_ruler( $line, $i );
      last RULERSCAN;
    }
  }

  unless( $format ) {
    croak "no horizontal rule line found: is this really db output data box format?"
  }

  # read data (with header) now that we know where things are
  my $last_data = $#lines;
  $last_data -= 1 if $format eq 'mysql';  # to skip that ruler line at bottom
  foreach my $i ( $header_loc, $first_data .. $last_data ) {
    my $line = $lines[ $i ];

    if( $format eq 'mysql' ) {
      # convert to postqres-style lines by trimming the borders
      $line =~ s{ $left_edge_rule  }{}xms;
      $line =~ s{ $right_edge_rule }{}xms;
    }

    my @vals;
    my $beg = 0;
    foreach my $pos ( @pos ) {
      binmode STDERR, ":encoding(UTF-8)" || die "binmode on STDERR failed";
      ($DEBUG) && printf STDERR "beg: %6d, pos: %6d, line: %s\n", $beg, $pos, $line;
      my $val =
        substr( $line, $beg, ($pos-$beg) );  # TODO why not use unpack?
      # strip leading and trailing spaces
      $val =~ s/^\s+//;
      $val =~ s/\s+$//;
      push @vals, $val;
      $beg = $pos + 1;
    }

    # array_of_array format (header in first line)
    push @data, \@vals;
  }
  my @header;
  @header = @{ $data[0] } if @data;
  $self->header( \@header );

  $self->format( $format );

  return \@data;
}



=item analyze_ruler

Internal method that analyzes the given ruler line and location
to determine column widths and the dbox format.

Returns an ordered list like so:

 format:
   'mysql', 'postgres', 'postgres_unicode', 'sqlite'

 header location:
   a row number: 0 or 1

 first_data:
   the row number where data begins:  2 or 3

 positions:
   a list of column boundary positions


Example usage:

  ( $format, $header_loc, $first_data, @pos ) = $self->analyze_ruler( $line, $i );

=cut

sub analyze_ruler {
  my $self = shift;
  my $ruler      = shift;
  my $ruler_loc  = shift;

  my $cross_rule      = $self->cross_rule;

  my ( $format, $header_loc, $first_data, @pos );

  if ( $ruler_loc == 2 ) {
    $format = 'mysql';
    $header_loc = 1;
    $first_data = 3;
  } elsif ( $ruler_loc == 1 ) {
    $header_loc = 0;
    $first_data = 2;
    if ( $ruler =~ $cross_rule ) {
      $format = 'postgres';
    } else {
      $format = 'sqlite';
    }
  }

  if ( $format eq 'mysql' ) {
    unless (
            $ruler =~ s{ ^ $cross_rule  }{}xms &&
            $ruler =~ s{ $cross_rule $  }{}xms
           ) {
      warn "mysql format, but ruler line was not terminated by crosses"
    }
  }

  # TODO identifying the cross could be combined with match to find horizontal rule
  my %cross_candidates =
    ( "\N{PLUS SIGN}"                                  => 'ascii', # ye olde '+'
      "\N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}" => 'unicode', # newfangled '┼'
      " "                                              => 'spaces',
    );

  my $cross;
  foreach my $candy ( keys %cross_candidates ) {
    my $pat = '\\' . $candy;    # need to backwhack the + char

    if ( $candy eq ' ' ) {
      $pat = qr{ \s{1,2} }x;
    }

    if ( $ruler =~ m{ $pat }x ) {
      $cross = $candy;
      # $meta{ encoding } = $cross_candidates{ $candy };

      $format .= '_' . 'unicode' if  $cross_candidates{ $candy } eq 'unicode';
    }
  }

  # 'index' be dumb:
  #    o  it can't use a regexp: it's limited to character matches.
  #    o  returns -1 on failure (what's wrong with undef?)
  my $pos = 0;
  while ( ( $pos = index( $ruler, $cross, $pos ) ) > -1 ) {
    push @pos, $pos;
    $pos++;
  }
  push @pos, length( $ruler ); # treat the eol as another column boundary

  # cleanup @pos: on immediately consecutive entries can drop the second one
  my $last = 0;
  my @newpos = ();
  foreach my $i ( 0 .. $#pos ) {
    my $this = $pos[ $i ];
    push @newpos, $this
      unless ( ($this-$last) == 1 );
    $last = $this;
  }
  @pos = @newpos;

  return ( $format, $header_loc, $first_data, @pos );
}



=item read_simple

This is DEPRECATED.  See L<read_dbox>.

Given data in tabular boxes from a multiline string,
convert it into an array of arrays.

   my $data =
         $bxs->read_simple();

Goes through the boxdata slurped into the object field input_data,
returns it as an array of arrays, including the field names in
the first row.

As a side-effect, stores the header (first row of boxdata)
in the object's L<header>.

=cut

# Early appoach: does regexp parsing of separator characters on each line
sub read_simple {
  my $self = shift;

  # the input file can be defined at the object level, or supplied as an argument
  # if it's an argument, the given value will be stored in the object level
  my $input_file;
  if( $_[0] ) {
    $input_file = shift;
    $self->input_file( $input_file );
  }

  my $input_data    = $self->input_data;
  # my $ruler_line_rule = $self->ruler_line_rule;
  my $separator_rule  = $self->separator_rule;

  # before we split on delimiters, trim the left and right borders (if any)
  # (converts mysql-style lines into psql-style lines)
   my $left_edge_rule  = $self->left_edge_rule;
   my $right_edge_rule = $self->right_edge_rule;

   $input_data =~ s{ ^ \s*        }{}xmsg;
   $input_data =~ s{   \s* $       }{}xmsg;

   $input_data =~ s{ $left_edge_rule  }{}xmsg;
   $input_data =~ s{ $right_edge_rule }{}xmsg;

  # Here we just look for lines with delimiters on them (skipping
  # anything else) and then split the lines on the delimiters,
  # trimming whitespace from the boundaries of all values

  my @lines = split /\n/, $input_data;

  my ( @data );
  for my $i ( 0 .. $#lines ) {
    my $line = $lines[ $i ];

    # when there's at least one delim, we assume it's a data line
    if( $line =~ /$separator_rule/xms ) {

      no warnings 'uninitialized';

      # need this for the whitespace not adjacent to delimiters...
      $line =~ s/^\s+//; # strip leading spaces
      $line =~ s/\s+$//; # strip trailing spaces (if any)

      # Note: split pattern also eats bracketing whitespace
      my @vals =
        split /$separator_rule/, $line;

      # array_of_array format (header treated like any other vals)
      push @data, \@vals;
    }

    my @header;
    @header = @{ $data[0] } if @data;
    $self->header( \@header );
  }
  return \@data;
}

=item output_to_tsv

A convenience method that runs L<read_dbox> and writes the data
to a tsv file specified by the given argument.  

Returns a reference to the data (array of arrays).

Example usage:

  $dbx->output_to_tsv( $input_dbox_file, $output_tsv_file );

Or:

  $dbx = Table::BoxFormat->new( input_file => $input_dbox_file );
  $dbx->output_to_tsv( $output_tsv_file );

Or:

  $dbx = Table::BoxFormat->new( input_data => $dbox_string );
  $dbx->output_to_tsv( $output_tsv_file );

=cut

# TODO if no output_file is supplied as argument, could fall back
#      to using the input_file with extension changed to "tsv".
sub output_to_tsv {
  my $self = shift;

  my $input_file;
  if( scalar( @_ ) == 2 ) {
    $input_file = shift;
    $self->input_file( $input_file );
  }

  my $output_file = shift;
  unless( $output_file ) {
    croak("output_to_tsv requires the output_file.");
  }
  my $output_encoding = $self->output_encoding;

#  my $data = $self->read_dbox;
  my $data = $self->data;

  my $out_enc = ">:encoding($output_encoding)";
  open my $fh, $out_enc, $output_file or die "$!";

  for my $i ( 0 .. $#{ $data } ) {
    my $line = join "\t", @{ $data->[ $i ] };
    print { $fh }  $line, "\n";
  }
#  return 1;
  return $data;
}


=item output_to_csv

A convenience method that runs L<read_dbox> and writes the data
to a csv file specified by the given argument.

Example usage:

  $dbx->output_to_csv( $input_dbox_file, $output_csv_file );

Or:

  $dbx = Table::BoxFormat->new( input_file => $input_dbox_file );
  $dbx->output_to_csv( $output_csv_file );

Or:

  $dbx = Table::BoxFormat->new( input_data => $dbox_string );
  $dbx->output_to_csv( $output_csv_file );

=cut

# TODO if no output_file is supplied as argument, could fall back
#      to using the input_file with extension changed to "csv".

sub output_to_csv {
  my $self = shift;

  # my $in_enc = "<:encoding($input_encoding)";

  my $input_file;
  if( scalar( @_ ) == 2 ) {
    $input_file = shift;
    $self->input_file( $input_file );
  }

  my $csv = Text::CSV->new ( { binary => 1 } )  # should set binary attribute.
    or die "Cannot use CSV: ".Text::CSV->error_diag ();

  my $output_file = shift;
  unless( $output_file ) {
    croak("output_to_csv requires the output_file.");
  }
  my $output_encoding = $self->output_encoding;

#  my $data = $self->read_dbox;
  my $data = $self->data;

  my $out_enc = ">:encoding($output_encoding)";
  open my $fh, $out_enc, $output_file or die "$!";

  for my $i ( 0 .. $#{ $data } ) {
###    my $line = join "\t", @{ $data->[ $i ] };
    my @columns = @{ $data->[ $i ] };
    my $status = $csv->combine(@columns);    # combine columns into a string
    my $line   = $csv->string();             # get the combined string

    print { $fh }  $line, "\n";
  }
  return 1;
}




=back

=head1 SEE ALSO

See L<Graphics::Skullplot>, which uses emacs lisp code to 
pick out a box format table from a field of text, e.g. a 
database shell window. 

=head1 AUTHOR

Joseph Brenner, E<lt>doom@kzsu.stanford.eduE<gt>,
05 Jun 2016

=head1 LIMITATIONS

=head2 memory limited

As implemented, this presumes the entire data set can be held in memory.
Future versions may be more stream-oriented: there's no technical reason
this couldn't be done.

=head2 what you get is what you get

This code is only guaranteed to cover input formats from mysql, psql
and some from sqlite3.  It may work with other databases, but
hasn't been tested.

At present it is not easily extensible (implementing a plugin
system ala DBI/DBD seemed like overkill).

=head2 sqlite3

This code does not support the default output from sqlite3,
it requires the use of these settings:

  .header on
  .mode column

While sqlite3 is very flexible, unfortunately the default output
does not seem very useable:

  SELECT * from expensoids;
  |2010-09-01|factory|146035.0
  |2010-11-01|factory|218866.0
  |2011-01-01|factory|191239.0
  |2010-10-01|marketing|409430.0

This is separated by the traditional ascii vertical bar, but
without the usual bracketing spaces, and without any attempt at
using fixed width columns.  Somewhat oddly, the left edge has a
vertical bar, but the right edge does not, but even worse there's
no header with column labels.

If you use the sqlite settings indicated above, you get a 
more conventional tabular text format:

  id          date        type        amount
  ----------  ----------  ----------  ----------
  1           2010-09-01  factory     146035.0
  2           2010-10-01  factory     208816.0
  3           2010-11-01  factory     218866.0

That's very similar to the psql format using "\pset border 0"
(though that has one space column breaks instead of two):
both are supported by L<read_dbox> using the L<analyze_ruler>
routine.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Joseph Brenner

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
