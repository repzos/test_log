#!/usr/bin/perl
#use warnings;
use strict;

use CGI;
use DBI; 
print "Content-type: text/html\n\n";
my $my_cgi = new CGI;
my $mail = $my_cgi->param('mail');

print "<b>Mail - $mail</b><br><br>"; 

my $dbh = DBI->connect("dbi:Pg:dbname=logs;host=127.0.0.1; port=5432","postgres","", {PrintError => 0})  or die "Couldn't connect to database: " . DBI->errstr;
my $query = "select distinct(int_id) from log  where address = '$mail'";
my $sth = $dbh->prepare($query);
my $rv = $sth->execute();

my @int_id;

while(my $int = $sth->fetchrow_array) {
	chomp $int;
	push @int_id, $int;
	}

@int_id = sort {$a <=> $b}  @int_id;

my $count;
for(@int_id){
	shift;
	$query = "select * from (select created,str from message  where int_id = '$_' UNION ALL select created,str from log  where int_id = '$_') a order by created";
	$sth = $dbh->prepare($query);
	$rv = $sth->execute();
	while(my @row = $sth->fetchrow_array) {
		$count ++;
		print "@row<br>";
		if($count >=100){
			print "<br>The log exceeds 100 records, the output is stopped<br>";
			exit 0;
			}
		}
	print "----------------------------------------------------------------------<br>";
	}

