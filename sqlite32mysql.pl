#!/usr/bin/perl -w
use strict;

use DBI;
use Getopt::Std;
use SQL::Translator;


use vars qw(%opts);

if (!$ARGV[1])
{
	print "usage: sqlite32mysql.pl [SQLite3 dump file] [output file]\n";
	exit 1;
}

my $file = $ARGV[0];
my $output_file = $ARGV[1];

my $translator          = SQL::Translator->new(
      # Print debug info
      debug               => 1,
      # Print Parse::RecDescent trace
      trace               => 0,
      # Don't include comments in output
      no_comments         => 1,
      # Print name mutations, conflicts
      show_warnings       => 0,
      # Add "drop table" statements
      add_drop_table      => 1,
      # to quote or not to quote, thats the question
      quote_table_names     => 1,
      quote_field_names     => 1,
      # Validate schema object
      validate            => 1,
      # Null-op formatting, only here for documentation's sake
      format_package_name => sub {return shift},
      format_fk_name      => sub {return shift},
      format_pk_name      => sub {return shift},
);


open(INPUT, "<", $file)  or die $!;
my @SQL = <INPUT>;
close(INPUT);

my $sql_lines;
my $insert_lines;
my $start;
my $end;

($sql_lines, $insert_lines, $start, $end) = lsSQLite2MySQL_pre(join("\n",@SQL));

my $output     = $translator->translate(
    from       => 'SQLite',
    to         => 'MySQL',
    data   => $sql_lines,
) or die $translator->error;


my $new_sql = lsSQLite2MySQL_post($output, $insert_lines, $start, $end);


open(SQLITE_DUMP, ">", $output_file);
print SQLITE_DUMP "$new_sql";
close(SQLITE_DUMP);

sub lsSQLite2MySQL_post {
	my $sql_schema = shift(@_);
	my $insert_lines = shift(@_);
	my $start = shift(@_);
	my $end = shift(@_);

	$sql_schema =~ s/DROP TABLE ('|"|`)?(\w+)('|"|`)?;/DROP TABLE IF EXISTS `$2`;/g; 
	
	my $toRet;
	
	$toRet = join("\n",$start,$sql_schema,$insert_lines,$end);
	$toRet;
}	
	
sub lsSQLite2MySQL_pre {
	my @sql = split /\n/, shift(@_);
	my $i;
	my @inserts;
	my $start = "";
	my $end = "";

	for my $line (@sql)
	{
		if ($line =~ /BEGIN\s+(DEFFERED\s+|IMMEDIATE\s+|EXCLUSIVE\s+)?TRANSACTION\s*;/)
		{
			my $clause;
			if (!$clause) { $clause = ""; } else { $clause = $1; }
#			$start = "BEGIN ".$clause."TRANSACTION;\n";
			$line = "";
		}
	
		if ($line =~ /(COMMIT|END)(\s+TRANSACTION)?\s*;/)
		{
			my $clause;
			if (!$2) { $clause = ""; } else { $clause = $2; }
#			$end = "$1$clause;\n";
			$line = "";
		}

		$line =~ s/DROP TABLE IF EXISTS ('|"|`)?(\w+)('|"|`)?;/DROP TABLE $2;/g; 
		
		if ($line =~ /^\s*INSERT INTO ('|"|`)(\w+)('|"|`)(.*)$/)
		{
			$line = "INSERT INTO `$2`$4";
			push(@inserts, $line);
			$line = "";
		}

		$line =~ s/\s*PRAGMA.*//g;
	}
	my $sql_lines = join("\n",@sql);
	$sql_lines =~ s/\n+/\n/g;

	my $insert_lines = join("\n", @inserts);
	$insert_lines =~ s/\n+/\n/g;

	($sql_lines,$insert_lines,$start,$end);
}
