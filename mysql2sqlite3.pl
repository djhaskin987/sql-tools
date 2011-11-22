#!/usr/bin/perl -wà
use strict;

use DBI;
use Getopt::Std;
use SQL::Translator;


use vars qw(%opts);

if (!$ARGV[1])
{
	print "usage: mysql2sqlite3.pl [MySQL dump file] [output file}\n";
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

my $output     = $translator->translate(
    from       => 'MySQL',
    to         => 'SQLite',
    # Or an arrayref of filenames, i.e. [ $file1, $file2, $file3 ]
    filename   => $file,
) or die $translator->error;


my $new_sql = lsMySQL2SQLite($output, $file);

open(SQLITE_DUMP, ">", $output_file);
print SQLITE_DUMP "$new_sql";
close(SQLITE_DUMP);

#my @table = $sql =~ /CREATE\s+TABLE\s+(\w+)/g;
#print "creating tables: ",join(' ',@table),"\n";

#print "$new_sql";

#my $dbh = DBI->connect(
#   ("DBI:SQLite:dbname=$db.dbm"),
#   {RaiseError=>1}
#);

#$dbh->do($new_sql);

sub lsMySQL2SQLite {

	my $sql_schema;

	my @Ssql = split /\n/,shift(@_);

	my $start;
	my $end;
	for $sql_schema (@Ssql)
	{
		# begin changes in the SQL schema
		$sql_schema =~ s/DROP TABLE (\w+);/DROP TABLE IF EXISTS $1;/g; 
		if ($sql_schema =~ /BEGIN\s+(DEFFERED\s+|IMMEDIATE\s+|EXCLUSIVE\s+)?TRANSACTION\s*;/)
		{
			my $clause;
			if (!$clause) { $clause = ""; } else { $clause = $1; }
			$start = "BEGIN ".$clause."TRANSACTION;\n";
			$sql_schema = "";
		}
	
		if ($sql_schema =~ /(COMMIT|END)(\s+TRANSACTION)?\s*;/)
		{
			my $clause;
			if (!$2) { $clause = ""; } else { $clause = $2; }
			$end = "$1$clause;\n";
			$sql_schema = "";
		}
		
		$sql_schema =~ s/\n+/\n/g;
	}
	
	
	# begin looking for inserts
	
	open(FILE, "<", shift(@_) ) or die $!;
	
	my @inserts;
	my @new_inserts;
    my $line;
	my $toRet;
	
	my @lines = <FILE>;
	
	for $line (@lines)
	{
		@new_inserts = checkForInsert($line);
		push(@inserts, @new_inserts);
	}
	
	$toRet = join("\n",$start,@Ssql,@inserts,$end);
	$toRet;
}	
	
sub checkForInsert {
		my $sql = shift(@_);
		my @inserts;
		if ($sql =~ /^\s*INSERT INTO (`|'|")(\w*)(`|'|") VALUES \((.*)\);$/)
		{
			my @records = split /\),\(/, $4;
			my $table = $2;

			for my $record (@records)
			{
				push(@inserts, "INSERT INTO '$table' VALUES ($record);");
			}
		}
		@inserts;
}
#sub parse_args {
#   my %opt = %{+shift};
#   return @opt{qw(h)};
#}

