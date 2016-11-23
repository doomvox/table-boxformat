package Data::BoxFormat;
use Moo;
use MooX::Types::MooseLike::Base qw(:all);

=head1 NAME

Data::BoxFormat - work with db SELECT results in tabular text format

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';
my  $DEBUG   = 1;          # TODO revise before shipping
use 5.008;
use Carp;
use Data::Dumper;
use utf8::all;

=head1 SYNOPSIS

   use Data::BoxFormat;
   # Reading input from a "dbox" temp file
   my $dbx = Data::BoxFormat->new( input_file => '/tmp/select_result.dbox' );
   my $data = $dbx->read(); # array of arrays, header in first row


   # Input from a string
   my $dbx = Data::BoxFormat->new( input_data => $dboxes_string );
   my $data = $dbx->read();


   # input from dbox file, output directly to a tsv file
   my $dbx = Data::BoxFormat->new( input_file  => '/tmp/select_result.dbox' );
   $dbx->read2tsv( '/tmp/select_result.tsv' );

   # TODO test this
   # Use just a unicode character as sepatator via a custom
   # parsing regexp (this allows '|' in string values).
   my $dbx =
      Data::BoxFormat->new( input_file => '/tmp/select_result.dbox',
                        delimiter_re =>
                          sub{ qr{
                                \s+
                                 \N{BOX DRAWINGS LIGHT VERTICAL}
                                    {1,1}
                                \s+
                              }xms } );
   my $data = $dbx->read(); # array of arrays, header in first row




=head1 DESCRIPTION

Data::BoxFormat is a module to work with data in the tabular text
format(s) commonly used in database client shells (postgresql's
"psql", mysql's "mysql", etc), where a SELECT will typical display
data in a form such as this (mysql):

  +-----+------------+---------------+-------------+
  | id  | date       | type          | amount      |
  +-----+------------+---------------+-------------+
  |  11 | 2010-09-01 | factory       |   146035.00 |
  |  12 | 2010-10-01 | factory       |   208816.00 |
  |  15 | 2011-01-01 | factory       |   191239.00 |
  |  16 | 2010-09-01 | marketing     |   467087.00 |
  |  17 | 2010-10-01 | marketing     |   409430.00 |
  +-----+------------+---------------+-------------+

Or this (postgresql's "ascii" form):

   id |    date    |   type    | amount
  ----+------------+-----------+--------
    1 | 2010-09-01 | factory   | 146035
    2 | 2010-10-01 | factory   | 208816
    4 | 2011-01-01 | factory   | 191239
    6 | 2010-09-01 | marketing | 467087
    7 | 2010-10-01 | marketing | 409430

These formats are human-readable, but not suitable for other
purposes such as feeding to a graphics program, or inserting into
another database table.

This code presumes these text tables of "data boxes" are either
held in a string or saved to a file.

By default this code can work with three different
formats: mysql, psql and unicode psql.

=head2 implementation notes

To write general code to work with all three formats, we
need to do three things:

(1) skip lines like this

  +-----+------------+---------------+-------------+
  ----+------------+-----------+--------
  ────┼────────────┼───────────┼────────

(2) treat either of these two characters as data delimiters,
  the ascii vertical bar or the unicode "BOX DRAWINGS LIGHT VERTICAL":
    |│

007C
007C;VERTICAL LINE;Sm;0;ON;;;;;N;VERTICAL BAR;;;;
|


(3) strip leading and trailing whitespace on each value

And we can cover some possible variations with some object-level
settings...

=head3 unicode characters

the unicode psql format uses these three characters:

uniprops U+2502
U+2502 ‹│› \N{BOX DRAWINGS LIGHT VERTICAL}
    \pS \p{So}
    All Any Assigned InBoxDrawing Box_Drawing Common Zyyy So S Gr_Base
       Grapheme_Base Graph GrBase Other_Symbol Pat_Syn Pattern_Syntax PatSyn
       Print Symbol Unicode X_POSIX_Graph X_POSIX_Print

uniprops U+2500
U+2500 ‹─› \N{BOX DRAWINGS LIGHT HORIZONTAL}
    \pS \p{So}
    All Any Assigned InBoxDrawing Box_Drawing Common Zyyy So S Gr_Base
       Grapheme_Base Graph GrBase Other_Symbol Pat_Syn Pattern_Syntax PatSyn
       Print Symbol Unicode X_POSIX_Graph X_POSIX_Print

uniprops U+253c
U+253C ‹┼› \N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}
    \pS \p{So}
    All Any Assigned InBoxDrawing Box_Drawing Common Zyyy So S Gr_Base
       Grapheme_Base Graph GrBase Other_Symbol Pat_Syn Pattern_Syntax PatSyn
       Print Symbol Unicode X_POSIX_Graph X_POSIX_Print

=head1 REGEXP PROPERTIES

Defining some custom regexp character properties.
(A neat trick, but for our purposes this gains us nothing:
these will go away in later versions.

=over

=cut

=item IsHor

Matches characters found in a "horizontal rule" row.

=cut

# defining character properties for regexp defaults
sub IsHor {
  my @codepoints =
    ('002D',  # -  \N{HYPHEN-MINUS}
     '002B',  # +  \N{PLUS SIGN}
     '2500',  # ─  \N{BOX DRAWINGS LIGHT HORIZONTAL}
     '253C',  # ┼  \N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}
     );
  my $list = join "\n", @codepoints;
  return $list;
}

=item IsCross

Matches the "cross" characters used at line intersections.

=cut

sub IsCross {
  my @codepoints =
    (
     '002B',  # +  \N{PLUS SIGN}
     '253C',  # ┼  \N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}
     );
  my $list = join "\n", @codepoints;
  return $list;
}

=item IsDelim

Matches the delimeter/separator characters used on column boundaries.

=cut

sub IsDelim {
  my @codepoints =
    (
     '007C',  # |  \N{VERTICAL LINE}
     '2502',  #  │  \N{BOX DRAWINGS LIGHT VERTICAL}
    );
  my $list = join "\n", @codepoints;
  return $list;
}

=back

=head1 METHODS

=over

=cut

=item new

Creates a new Data::BoxFormat object.

Takes a list of attribute/setting pairs as an argument.

=over

=item input_file

File to input data from.  Only required if L<input_data> was not
defined directly.

=item input_encoding

Default's to "UTF-8".  Change to suit text encoding (e.g. "ISO-8859-1").
Must work as a perl ":encoding(...)" layer.

=item input_data

SQL SELECT output in the fixed-width-plus-delimiter form discussed above.

=item output_encoding

Like L<input_encoding>.  Default: "UTF-8".

=item the parsing regular expressions (type: RegexpRef)

=over

=item delimiter_re

The column separators (vertical bar)
TODO rename this: separator, not delimiter.

=item horizontal_re

Horizontal bars inserted for readability

=item cross_re

Match cross marks the horizontal bars typically use to mark
column boundaries (not yet in use).

=item left_edge_re

Left border delimiters (we strip these before processing).

=item right_edge_re

Right border delimiters (we strip these before processing).

=back

=back

=cut

# Example attribute:
# has is_loop => ( is => 'rw', isa => Int, default => 0 );

# input file name (can skip if input_data is defined directly)
has input_file  => ( is => 'rw', isa => Str, default => "" );

# input encoding defaults to utf-8 (might need to change, e.g. ISO-8859-1)
has input_encoding => ( is => 'rw', isa => Str, default => 'UTF-8' );

# # only used by read2tsv (at present)
# has output_file     => ( is => 'rw', isa => Str, default => "" );
# ### TODO lazy default, generate from the input_file, change extension to tsv

has output_encoding => ( is => 'rw', isa => Str, default => 'UTF-8' );

# can define input data directly, or alternately slurp it in from a file
has input_data  => ( is => 'rw', isa => Str,
           default =>
             sub { my $self = shift;
                   my $input_file = $self->input_file;
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
                   return $data;
                 },
            lazy => 1 );
# TODO would be better if it avoided slurping, and worked line-at-a-time

has header => ( is => 'rw', isa => ArrayRef, default => sub{ [] } );

# TODO
# but don't *really* want to allow mix-and-match delims, it's always one or the other throughout.
has delimiter_re  => ( is => 'rw', isa => RegexpRef,
                       default =>
                       sub{ qr{
                                \s+         # require leading whitespace
                                \p{IsDelim}
                                    {1,1}   # just one delim char
                                \s+         # require trailing whitespace
                            }xms } );


has horizontal_re => ( is => 'rw', isa => RegexpRef,
                       default => sub{ qr{ ^ \p{IsHor}+  $ }x } );

# TODO
#   horizontal rules have crosses that mark-the-spots of col borders
#   use this piece of information: fixed width fields, delims are lined-up
has cross_re  => ( is => 'rw', isa => RegexpRef,
                       default =>
                       sub{ qr{
                                \p{IsCross}
                                    {1,1}   # just one delim char
                            }xms } );

# To match table borders (e.g. mysql-style)
has left_edge_re => ( is => 'rw', isa => RegexpRef,
                       default => sub{ qr{ ^ \s* [\|] }xms } );

has right_edge_re => ( is => 'rw', isa => RegexpRef,
                       default => sub{ qr{ [\|] \s* $ }xms } );


=item read

Given data in tabular boxes from a multiline string,
convert it into an array of arrays.

   my $data =
         $bxs->read();

Goes through the boxdata slurped into the object field input_data,
returns it as an array of arrays, including the field names in
the first row.

As a side-effect, stores the header (first row of boxdata)
in the object's L<header>.

=cut

### TODO be more forgiving of values with embedded delim chars.
sub read {
  my $self = shift;

  my $input_data    = $self->input_data;
  my $horizontal_re = $self->horizontal_re;  # intended to skip these... TODO
  my $delimiter_re  = $self->delimiter_re;

  # before we split on delimiters, trim the left and right borders (if any)
  # (converts mysql-style lines into psql-style lines)
   my $left_edge_re  = $self->left_edge_re;
   my $right_edge_re = $self->right_edge_re;

# BUG (?): will never match, because of the \+ stuff
#    $input_data =~ s{ ^ \s* \+       }{}xmsg;
#    $input_data =~ s{ \+ \s* $       }{}xmsg;
   $input_data =~ s{ ^ \s*        }{}xmsg;
   $input_data =~ s{   \s* $       }{}xmsg;


   $input_data =~ s{ $left_edge_re  }{}xmsg;
   $input_data =~ s{ $right_edge_re }{}xmsg;

  # Here we just look for lines with delimiters on them (skipping
  # anything else) and then split the lines on the delimiters,
  # trimming whitespace from the boundaries of all values

  my @lines = split /\n/, $input_data;

  my ( @data );
  for my $i ( 0 .. $#lines ) {
    my $line = $lines[ $i ];

    # when there's at least one delim, we assume it's a data line
    if( $line =~ /$delimiter_re/xms ) {

      no warnings 'uninitialized';

      # need this for the whitespace not adjacent to delimiters...
      $line =~ s/^\s+//; # strip leading spaces
      $line =~ s/\s+$//; # strip trailing spaces (if any)

      # Note: split pattern also eats bracketing whitespace
      my @vals =
        split /$delimiter_re/, $line;

      # array_of_array format (header treated like any other vals)
      push @data, \@vals;
    }

    my @header;
    @header = @{ $data[0] } if @data;
    # print STDERR Dumper( \@header ), "\n"; # TODO when turned on there's an odd instance in tests where an empty one comes up
    $self->header( \@header );
  }
  return \@data;
}


# Experimental version: try to develop "parse" that
# uses all information, including column locations.
# Also, could try to do it single-pass (or closer to it).
# Compromise: line-at-a-time but allow backtracking on a line.
sub read_exp {
  my $self = shift;

  my $input_data    = $self->input_data;
  my $horizontal_re = $self->horizontal_re;
  my $delimiter_re  = $self->delimiter_re;

  my $cross_re      = $self->cross_re;

   my $left_edge_re  = $self->left_edge_re;
   my $right_edge_re = $self->right_edge_re;

  # Here we just look for lines with delimiters on them (skipping
  # anything else) and then split the lines on the delimiters,
  # trimming whitespace from the boundaries of all values

  my @lines = split /\n/, $input_data;

  my ( @data, @dividers );

  # we use the crosses in the horizontal rule line to identify column boundaries
  my @pos;
 HEADSCAN:
  foreach my $line ( @lines ) { # TODO: $line is aliased back into @lines...
#     # strip leading and trailing spaces (as in the next loop-- see below)
#     $line =~ s{ ^ \s*        }{}xms;
#     $line =~ s{   \s* $      }{}xms;

    if( $line =~ m{ $horizontal_re }x ) {

      # on mysql-style, want to strip leading and trailing crosses
      $line =~ s{ ^ $cross_re  }{}xms;
      $line =~ s{ $cross_re $  }{}xms;

      # 'index' be dumb:
      #  o  it can't use a regexp: it's limited to character matches.
      #  o  returns -1 on failure (what's wrong with undef?)

      my @cross_candy =
        ( "\N{PLUS SIGN}",  # ye olde ascii '+'
          "\N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}" ); # newfangled '┼'

      my $cross;
      foreach my $candy ( @cross_candy ) {
        my $pat = '\\' . $candy;  # need to backwhack the + char
        if ( $line =~ m{ $pat }x ) {
          $cross = $candy;
        }
      }

      my $pos = 0;
      while( ( $pos = index( $line, $cross, $pos ) ) > -1 ){
        push @pos, $pos;
        $pos++;
      }
      last HEADSCAN;
    }
  }

 LINE:
  foreach my $line ( @lines ) {
#     # strip leading and trailing whitespace
#     $line =~ s{ ^ \s*        }{}xms;
#     $line =~ s{   \s* $      }{}xms;

    # trim the left and right borders (if any)
    # (converts mysql-style lines into psql-style lines)
    $line =~ s{ $left_edge_re  }{}xms;
    $line =~ s{ $right_edge_re }{}xms;

    next LINE if( $line =~ m{ $horizontal_re }x );

    my @vals;
    my $beg = 0;
    foreach my $pos ( @pos ) {
      my $val =
        substr( $line, $beg, ($pos-$beg-1) );
      # strip leading and trailing spaces
      $val =~ s/^\s+//;
      $val =~ s/\s+$//;
      push @vals, $val;
      $beg = $pos + 1;
    }

    # Now do the last one (through the end of the line)
    my $val =
      substr( $line, $beg );
    # strip leading and trailing spaces
    $val =~ s/^\s+//;
    $val =~ s/\s+$//;
    push @vals, $val;

    # array_of_array format (header treated like any other vals)
    push @data, \@vals;

    my @header;
    @header = @{ $data[0] } if @data;
    $self->header( \@header );
  }
  return \@data;
}



=item read2tsv

A convenience method that runs L<read> and writes the data to a
tsv file specified by the given argument.

=cut

# TODO if no output_file is supplied as argument, could fall back
#      to using the input_file with extension changed to "tsv".
sub read2tsv {
  my $self = shift;

  my $output_file = shift;
  unless( $output_file ) {
    croak("read2tsv requires the output_file.");
  }
  my $output_encoding = $self->output_encoding;

  my $data = $self->read;

  my $out_enc = ">:encoding($output_encoding)";
  open my $fh, $out_enc, $output_file or die "$!";

  for my $i ( 0 .. $#{ $data } ) {
    my $line = join "\t", @{ $data->[ $i ] };
    print { $fh }  $line, "\n";
  }
  return 1;
}




=back

=head1 AUTHOR

Joseph Brenner, E<lt>doom@kzsu.stanford.eduE<gt>,
05 Jun 2016

=head1 LIMITATIONS

=head2 memory limited

As implemented, this presumes the entire data set can be held in memory.
Future versions may be more stream-oriented: there's no technical reason
this couldn't be done.

=head2 delimiters in data could confuse things

If delimiter characters (e.g. a veritcal bar) are present in a
string in the data, that could confuse the simple version of
parsing that this does (particularly if the embedded delimiter
char was bracketed by whitespace).

=head2 what you get is what you get

This only covers three input formats, and isn't easily extensible
to handle any others.  A plugin system (ala DBI/DBD) would be overkill.

=head2 sqlite

This code does not support the output from sqlite.
Let me explain why (so I'll stop thinking about adding it):

The output format in sqlite3 is actually fairly flexible,
it has a number of different customization settings.
The obvious thing to want to support though, is the default format,
which looks like this:

  SELECT * from expensoids;
  |2010-09-01|factory|146035.0
  |2010-10-01|factory|208816.0
  |2010-11-01|factory|218866.0
  |2010-12-01|factory|191239.0
  |2011-01-01|factory|191239.0
  |2010-09-01|marketing|467087.0
  |2010-10-01|marketing|409430.0

This is delimited by an ascii vertical bar, but without any
bracketing spaces, and without any attempt at using fixed width
columns.  Note that the left edge has a vertical bar, but the
right edge does not.  Perhaps the most annoying feature from my
point of view though, is that there is no header: it's easy
enough to write code that assumes that there will never be a
header, and to just leave the header row blank, but if I were
actually working with sqlite a lot I would turn on the header
display:

  .header on

Trying to distguish whether the first row is a header is not
something I'd want try to do from textual analysis, but adding
some other way of doing it is not something I want to bother with
just to support sqlite.  So the question would be: should I assume
the default format, or the format I'm more likely to use?

But if I *were* working with sqlite a lot I wouldn't stop with
turning on the header, I'd probably use the mode column setting
also:

  .header on
  .mode column

That yields output that looks like this:

  id          date        type        amount
  ----------  ----------  ----------  ----------
  1           2010-09-01  factory     146035.0
  2           2010-10-01  factory     208816.0
  3           2010-11-01  factory     218866.0

And that's sufficiently different from everything else in the
world, and far enough away from the sqlite default most people
are likely to use, that I'm going to give up on this entirely.

One of the problems here for me is that I simply don't work with
sqlite very much (why would you, when you could be using
postgresql?), so I'm resolving to not think about this again.

=head1 ACKNOWLEDGEMENTS

I stand on the shoulders of leprechauns.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Joseph Brenner

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
