#!/usr/bin/perl 
use warnings;
use strict;
use DBI; 

my $file = "./out";

open(FH, '<', $file) or die $!;

my %message;
my %log;
my $id;

while (<FH>) {
	chomp $_;
	$_ =~ s/\\n//g;
	my $my = $_;
	my @arr = split(/\s/,$_);
	my $size = @arr;
	if ($arr[3] =~ /<\=/ and $arr[4] =~ /\w+\@\w+.\w+/){
		unless (exists $message{$arr[2]}){
			$message{$arr[2]}{id} = $id++;
			push @{$message{$arr[2]}{data}},  $_;
			}
		}elsif ($arr[3] =~ /\=\=|\*\*|>|</){
			if(exists $message{$arr[2]}){
				$log{$arr[2]}{id} = $message{$arr[2]}{id};
				push @{$log{$arr[2]}{data}},  $_;
				}else{
					unless (exists $message{$arr[2]}){
						$log{$arr[2]}{id} = $id++;
						push @{$log{$arr[2]}{data}},  $_;
						}
					}
			}else{
				unless ($arr[2] =~ /\w.....\-\w.....\-\w./){
					#next "SMTP connection"
					next;
					}
				if (exists $message{$arr[2]}){
					push @{$log{$arr[2]}{data}},  $_;
					}else{
						if(exists $log{$arr[2]}){
							push @{$log{$arr[2]}{data}},  $_;
							next;
							}else{
								$log{$arr[2]}{id} = $id++;
								push @{$log{$arr[2]}{data}},  $_;
								}
						}
				}
		}

#part two

my $dbh = DBI->connect("dbi:Pg:dbname=logs;host=127.0.0.1; port=5432","postgres","", {PrintError => 0})  or die "Couldn't connect to database: " . DBI->errstr;

foreach my $k(sort keys %message){
	if($message{$k}{data} =~ /ARRAY/){
		for(@{$message{$k}{data}}){
			&rec_db_message($message{$k}{id},$_);
			}
		}
	}	

foreach my $k(sort keys %log){
	if($log{$k}{data} =~ /ARRAY/){
		for(@{$log{$k}{data}}){
			&rec_db_log($log{$k}{id},$_);
			}
		}
	}		
$dbh->disconnect;

sub rec_db_message{
	chomp @_;
	my ($id,$data) = @_;
	my @arr = split(/\s/,$data);
	my $size = @arr;
	my @dt = splice @arr, 0, 2;
	my $str = join(" ", @arr);
	$str =~ s/\'/''/g;	
	my $date = join(" ",@dt);	
	my $query ="INSERT INTO message (created,id,int_id,str)	VALUES ('$date','$arr[0]','$id','$str')";
	$dbh->do($query);
	}

sub rec_db_log{
	chomp @_;
	my ($id,$data) = @_;
	my @arr = split(/\s/,$data);
	my $size = @arr;
	my @dt = splice @arr, 0, 2;
	my $str = join(" ", @arr);
	$str =~ s/\'/''/g;	
	my $date = join(" ",@dt);	
	if($arr[2] and $arr[2] =~ /\w+\@\w+.\w+/){
		my $query ="INSERT INTO log (created,int_id,str,address)	VALUES ('$date','$id','$str','$arr[2]')";
		my $rv = $dbh->do($query);
		print "$query\n" if ($rv ne 1);
		}else{
			my $query ="INSERT INTO log (created,int_id,str)	VALUES ('$date','$id','$str')";
			$dbh->do($query);
			}
	}	
exit 0;
