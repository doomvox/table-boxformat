#!/usr/bin/perl
# skullplot.pl                   doom@kzsu.stanford.edu
#                                27 Jul 2016

=head1 NAME

skullplot.pl - plot data from a manual db select

=head1 SYNOPSIS

  # default to first column on x-axis, all following columns on y-axis
  skullplot.pl input_data.dbox

  # specifying that explicitly
  skullplot.pl --dependents='x_axis_field' --independents='y_axis_field1,y_axis_field2' input_data.dbox

  # additional "dependents" fields determine color/shape of points
  skullplot.pl --dependents='x_axis_field1,category_field1' --independents='y_axis_field1,y_axis_field2' input_data.dbox

  # compact way of specifying similar case: first two columns independent, remaining columns dependent
  skullplot.pl --indie_count=2 input_data.dbox

  # don't use /tmp as working area
  skullplot.pl --working_loc='/var/scratch'  input_data.dbox

  # turn on debugging
  skullplot.pl -d  input_data.dbox

=head1 DESCRIPTION

B<skullplot.pl> is a script which use's the R ggplot2 library to
plot data input in the form of the output from a SELECT as
performed manually in a db shell, e.g.:

  +------------+---------------+-------------+
  | date       | type          | amount      |
  +------------+---------------+-------------+
  | 2010-09-01 | factory       |   146035.00 |
  | 2010-10-01 | factory       |   208816.00 |
  | 2011-01-01 | factory       |   191239.00 |
  | 2010-09-01 | marketing     |   467087.00 |
  | 2010-10-01 | marketing     |   409430.00 |
  +------------+---------------+-------------+

I call this "box format data" (file extension: *.dbox).

This script takes the name of the *.dbox file containing the data as an argument.
It has a number of options that control how it uses the data


The second argument is a comma-separated list of names of dependent variables
(the x-axis).
The third argument is a comma-separated list of the independent variables to
plot (the y-axis).

The default for dependent variables: the first column.
The default of independent variables: all of the following columns

The supported input data formats are as in the L<Data::BoxFormat> module.
At present, this is mysql and postgresql (including the unicode form).

=cut

use warnings;
use strict;
$|=1;
use Carp;
use Data::Dumper;

use File::Path      qw( mkpath );
use File::Basename  qw( fileparse basename dirname );
use File::Copy      qw( copy move );
use autodie         qw( :all mkpath copy move ); # system/exec along with open, close, etc
use Cwd             qw( cwd abs_path );
use Env             qw( HOME );
use List::MoreUtils qw( any );
use String::ShellQuote qw( shell_quote_best_effort );
use Config::Std;
use Getopt::Long    qw( :config no_ignore_case bundling );
use List::Util      qw( first max maxstr min minstr reduce shuffle sum );

use utf8::all;

our $VERSION = 0.01;
my  $prog    = basename($0);

my $DEBUG   = 0;
my $working_area = "/tmp/.skullplot";   # default
my ($dependent_spec, $independent_spec, $indie_count, $image_viewer);

GetOptions ("d|debug"    => \$DEBUG,
            "v|version"  => sub{ say_version(); },
            "h|?|help"   => sub{ say_usage();   },

           "dependents=s"     => \$dependent_spec,    # the x-axis, plus any gbcats
           "independents=s"   => \$independent_spec,  # the y-axis

           "indie_count=i" => \$indie_count,          # alt spec: indies=x1+gbcats; residue are ys

           "image_viewer=s" => \$image_viewer,        # default: ImageMagick's display (if available)

           "working_area=s"   => \$working_area,
           ) or say_usage();


unless( -d $working_area ) {
  mkpath( $working_area );
}


# TODO for development only.  Remove when shipped.
use FindBin qw( $Bin );
use lib ("$Bin/../lib/");

use Data::BoxFormat;

my $dbox_file = shift;

unless( $dbox_file ) {
  die "An input data file (*.dbox) is required.";
}

if( $dependent_spec && not( $independent_spec ) ) {
  die "When using dependents option, also need independents.";
} elsif( $independent_spec && not( $dependent_spec ) ) {
  die "When using independents option, also need dependents.";
} elsif( $indie_count && $dependent_spec) {
  die "Use either indie_count or dependents/independents options, not both.";
}

if ( $dependent_spec ) {
  ($DEBUG) &&
    print STDERR "Using independents: $independent_spec and dependents: $dependent_spec\n";
} elsif( $indie_count )  {
  ($DEBUG) &&
    print STDERR "Given indie_count: $indie_count\n";
} else {
  $indie_count = 1;
}

### generate a tsv file in the working area, along with rscript

my $dbox_name = basename( $dbox_file );
( my $tsv_name     = $dbox_name ) =~ s{ \.dbox $ }{.tsv}x;
( my $rscript_name = $dbox_name ) =~ s{ \.dbox $ }{.t}x;

my $tsv_file =     "$working_area/$tsv_name";
my $rscript_file = "$working_area/$rscript_name";

($DEBUG) && print "input dbox name: $dbox_name\nintermediate tsv_file: $tsv_file\n";

# input from dbox file, output directly to a tsv file
my $dbx = Data::BoxFormat->new( input_file  => $dbox_file );
$dbx->read2tsv( $tsv_file );

# use first col as the default independent variable (x-axis)
my @header = @{ $dbx->header() };
my $independent_default  = shift( @header );

# my $dependent_default    = join ',', @header;

my ($dep_list, $indep_list);
my ( @dep_fields, @indep_fields, @gb_cats, $x_axis );
if( $indie_count ) {
  $x_axis = $independent_default;
  @gb_cats      = @header[ 0 .. $indie_count-1 ];
  @indep_fields = @header[ $indie_count .. $#header ];
} elsif ( $dependent_spec || $independent_spec )  {
  $dep_list     = $dependent_spec   || join ',', @header;
  $indep_list   = $independent_spec || $independent_default;
  @dep_fields   = split /[,\|]/, $dep_list;
  @indep_fields = split /[,\|]/, $indep_list;
  $x_axis       = shift( @indep_fields );
  @gb_cats      = @dep_fields;
}

my $y_field = $indep_fields[ 0 ];  ### TODO DEBUG just ploting the first to start with
print STDERR "x_axis: $x_axis\n";
print STDERR "y_field: $y_field\n";
# my $plot_code = 'qplot( skull$' . $x_axis . ', skull$' . $y_field . ' )';

# TODO
# At present, limited to two group by categories (in addition to the x-axis)
# Maybe: if there's more than 2, fuse them together into a compound cat, hand to colour
my $gb_cat1 = $gb_cats[0] if $gb_cats[0];
my $gb_cat2 = $gb_cats[1] if $gb_cats[1];

# plot code
my $pc = 'ggplot( skull, ' ;
  $pc .= '               aes(' ;
  $pc .= "                    x = $x_axis," ;
  $pc .= "                    y = $y_field, " ;
  $pc .= "                    colour = $gb_cat1," if $gb_cat1;
  $pc .= "                    shape  = $gb_cat2 " if $gb_cat2;
  $pc .= '                          ))' ;
  $pc .= ' + geom_point( ' ;
#  $pc .=  "             shape = 21, " ;
  $pc .= "              size  = 2.5 " ;
  $pc .= '              )  ' ;

(my $png_file = $tsv_file ) =~s{ \.tsv $ }{.png}x;

### Generate R code file to run with Rscript call
### (in debug mode, make it a standalone unix script)
my $r_code;
$r_code = qq{#!/usr/bin/Rscript} . "\n" if $DEBUG;

$r_code .=<<"END";
library(ggplot2)
skull <- read.delim("$tsv_file", header=TRUE)
png("$png_file") # send plot output to png
$pc
graphics.off()   # doesn't chatter like dev.off
END

print $r_code, "\n" if $DEBUG;


open my $out_fh, '>', $rscript_file;
print { $out_fh } $r_code;
close( $out_fh );

# in case you want to run the rscript standalone
chmod 0755, $rscript_file;

# chdir( "$HOME/tmp" ) or die "$!";

my $erroff = '2>/dev/null';
$erroff = '' if $DEBUG;

my $cmd = "Rscript $rscript_file $erroff";

print STDERR "cmd:\n$cmd\n" if $DEBUG;
system( $cmd );

  # TODO make choice of display app settable...
  #      have an intelligent default: look at what's available.

# Running a desktop app is faster than R's browseURL:
# system("gthumb $png_file    $erroff");
# system("eog $png_file       $erroff");
# system("display $png_file   $erroff");  # ImageMagick
# exec(qq{ display  $png_file $erroff }); # ImageMagick
# exec(qq{ display -title 'skullplot'  $png_file $erroff }); # ImageMagick

# (Defaulting to ImageMagick's "display" is good, because I can
# use a dependency on Perlmagick to ensure that it's available)

my $vcmd;
if( $image_viewer ) {
  $vcmd = qq{ $image_viewer $png_file $erroff };
} else {
  $vcmd = qq{ display -title 'skullplot'  $png_file $erroff };
}

exec( $vcmd );


#######
### end main, into the subs

sub say_usage {
  my $usage=<<"USEME";
  $prog -[options] [arguments]

  Options:
     -d          debug messages on
     --debug     same
     -h          help (show usage)
     -v          show version
     --version   show version

TODO add additional options

USEME
  print "$usage\n";
  exit;
}

sub say_version {
  print "Running $prog version: $VERSION\n";
  exit 1;
}


__END__

=head1 AUTHOR

Joseph Brenner, E<lt>doom@kzsu.stanford.eduE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Joseph Brenner

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=head1 BUGS

None reported... yet.

=cut
