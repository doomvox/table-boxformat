package Data::BoxFormat;
use Moo;
use MooX::Types::MooseLike::Base qw(:all);

=head1 NAME

Data::BoxFormat - work with db SELECT results in tabular text format

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';
my $DEBUG = 1;              # TODO revise before shipping

=head1 SYNOPSIS

   use Data::BoxFormat;
   # Reading input from a "dbox" temp file
   my $dbx = Data::BoxFormat->new( input_file => '/tmp/select_result.dbox' );
   my $data = $dbx->read(); # array of arrays, header in first row


   # Input from a string
   my $dbx = Data::BoxFormat->new( input_data => $dboxes_string );
   my $data = $dbx->read();


   # input from dbox file, output directly to a tsv file
   my $dbx = Data::BoxFormat->new( input_file  => '/tmp/select_result.dbox',
                               output_file => '/tmp/select_result.tsv',
                             );
   $dbx->read2tsv();


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
                            }xms }
                       );
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




=head1 METHODS

=over

=cut

use 5.008;
use Carp;
use Data::Dumper;
use utf8::all;

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

=item output_file

Like L<input_file>, only used by L<read2tsv> (at present)

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

# only used by read2tsv (at present)
has output_file     => ( is => 'rw', isa => Str, default => "" );
### TODO lazy default, generate from the input_file, change extension to tsv

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

sub IsCross {
  my @codepoints =
    (
     '002B',  # +  \N{PLUS SIGN}
     '253C',  # ┼  \N{BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL}
     );
  my $list = join "\n", @codepoints;
  return $list;
}

sub IsDelim {
  my @codepoints =
    (
     '007C',  # |  \N{VERTICAL LINE}
     '2502',  #  │  \N{BOX DRAWINGS LIGHT VERTICAL}
    );
  my $list = join "\n", @codepoints;
  return $list;
}

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
                       default => sub{ qr{ \p{IsHor}+ }x } );

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


### TODO add features to look for bad data formats
### TODO even better: be more forgiving of values with embedded delim chars.
sub read {
  my $self = shift;

  my $input_data    = $self->input_data;
  my $horizontal_re = $self->horizontal_re;  # intended to skip these... TODO
  my $delimiter_re  = $self->delimiter_re;

  # before we split on delimiters, trim the left and right borders (if any)
  # (converts mysql-style lines into psql-style lines)
   my $left_edge_re  = $self->left_edge_re;
   my $right_edge_re = $self->right_edge_re;

   $input_data =~ s{ ^ \s* \+       }{}xmsg;
   $input_data =~ s{ \+ \s* $       }{}xmsg;
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



=item read2tsv

A convenience method that runs L<read> and writes the data to a
tsv file specified by the field L<output_file>.

=cut

sub read2tsv {
  my $self = shift;

  my $output_file = $self->output_file;
  unless( $output_file ) {
    croak("read2tsv requires the output_file.");
    ## TODO could use the input_file (if defined) changing the extension to tsv.
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

=head1 BUGS

Please report any bugs or feature requests to
C<bug-data-boxes at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-BoxFormat>.

I will be notified, and then you'll automatically be notified of
progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Emacs::Run

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Emacs-Run>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Emacs-Run>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Emacs-Run>

=item * Search CPAN

L<http://search.cpan.org/dist/Emacs-Run/>

=back

=cut

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
