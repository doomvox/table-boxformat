# Before 'make install' is performed this script should be runnable with
# 'make test'. After 'make install' it should work as 'perl Data-Boxes-Format-psql.t'

use strict;
use warnings;

use Data::Dumper;

use Test::More;
BEGIN {
  use FindBin qw( $Bin );
  use lib ("$Bin/../lib/");
  use_ok('Data::BoxFormat');
};


my $DAT = "$Bin/dat";
   #  /home/doom/End/Cave/SkullPlot/Wall/Data-Boxes/t/dat

{
  my $test_name = "Testing read method";


  my $expected = [
          [ 'id', 'date',       'type',      'amount' ],
          [  '1', '2010-09-01', 'factory',   '146035' ],
          [  '2', '2010-10-01', 'factory',   '208816' ],
          [  '3', '2010-11-01', 'factory',   '218866' ],
          [  '4', '2010-12-01', 'factory',   '191239' ],
          [  '5', '2011-01-01', 'factory',   '191239' ],
          [  '6', '2010-09-01', 'marketing', '467087' ],
          [  '7', '2010-10-01', 'marketing', '409430' ]
        ];


  my $format = 'psql';

  my $input_file = "$DAT/expensoids-psql.dbox";
  my $bxs =
    Data::BoxFormat->new(
                     input_file => $input_file,
                    );

  my $data = $bxs->read; # array of arrays, header in first row

  # print Dumper( $data ) , "\n";

  is_deeply( $data, $expected, "$test_name on $format format" );


  ###
  $format = 'psql_unicode';

  $input_file = "$DAT/expensoids-psql_unicode.dbox";

  $bxs =
    Data::BoxFormat->new(
                     input_file => $input_file,
                    );

  $data = $bxs->read; # array of arrays, header in first row

  is_deeply( $data, $expected, "$test_name on $format format" );

  ###
  $format = 'mysql';

  $input_file = "$DAT/expensoids-mysql.dbox";

  $bxs =
    Data::BoxFormat->new(
                     input_file => $input_file,
                    );

  $data = $bxs->read; # array of arrays, header in first row

  is_deeply( $data, $expected, "$test_name on $format format" );

}

{
  my $test_name = "Testing read2tsv method";

  my $format = 'psql_unicode';

  my $input_file  = "$DAT/expensoids-psql_unicode.dbox";
  my $output_file = "$DAT/expensoids-psql_unicode.tsv";
  my $bxs =
    Data::BoxFormat->new(
                     input_file  => $input_file,
                     output_file => $output_file,
                    );

  my $status = $bxs->read2tsv; # output straight to tsv file
  is( $status, 1, "$test_name: returns success code" );

  my $expected_file = qq{$DAT/expensoids_expected.tsv};
  my $expected = do{ undef $/; open my $fh, '<', $expected_file; <$fh> };
  my $result   = do{ undef $/; open my $fh, '<', $expected_file; <$fh> };

  is_deeply( $result, $expected, "$test_name on $format format" );
}

done_testing();
