#!/usr/bin/perl -w
#
use strict;
use warnings;
#
use XML::LibXML;
use threads;
use Data::Dumper;
use Benchmark;
use Sema4;

my $VERSION = '0.0.0.1';
my $DEBUG   = 1;
my $DIE_ON_WARN = 1;
my $CSV_SEPARATOR = ';';
#
my $log = '/tmp/xml2csv.log';
my %xml_dir = (
#       '/tmp/t_xml/' => '/tmp/t_csv/',
       '/data/DiallerData/Calls_u/' => '/tmp/t_csv/'
);

my %nodes = (
	'Type'=> 0,
	'SampleID'=> 1,
	'TelNum'=> 2,
	'DataBaseID'=> 3,
	'InterviewID'=> 4,
	'CallerID'=> 5,
	'ID'=> 6,
	'CampaignID'=> 7,
	'CampaignName'=> 8,
	'CampaignDisplayName'=> 9,
	'DiallerName'=> 10,
	'AgentID'=> 11,
	'AgentName'=> 12,
	'AgentDisplayName'=> 13,
	'ChannelName'=> 14,
	'ServerName'=> 15,
	'PortName'=> 16,
	'PortID'=> 17,
	'Outcome'=> 18,
	'DisconnectReason'=> 19,
	'RawDisconnectReason'=> 20,
	'Code'=> 21,
	'CodeDescriptor'=> 22,
	'Recorded'=> 23,
	'RecordingFile'=> 24,
	'Date'=> 25,
	'TimeSupply'=> 26,
	'TimeMake'=> 27,
	'TimeNonISDN'=> 28,
	'TimeRingback'=> 29,
	'TimeFail'=> 30,
	'TimeConnect'=> 31,
	'TimeDisconnect'=> 32,
	'TimeIdle'=> 33,
	'TimeComplete'=> 34,
	'SupplyToMake'=> 35,
	'MakeToNonISDN'=> 36,
	'MakeToRingback'=> 37,
	'MakeToFail'=> 38,
	'RingbackToConnect'=> 39,
	'RingbackToFail'=> 40,
	'ConnectToDisconnectIdle'=> 41,
	'DisconnectToIdle'=> 42,
	'IdleToComplete'=> 43,
);

print ">> ". keys( %nodes )."\n";
my $threadCount = Sema4->new(0);

open (LOG, ">>$log") or die "Can't open log file: $log\n";
print LOG "\n:::::::::::::::::::::::::::::::::::::::::::::::\n";
my @threads;
foreach my $path (keys %xml_dir ) {
	print LOG "-> $path\t" if $DEBUG;
	my @fileList = `find $path -type f -print`;
	print LOG $#fileList+1 ." files\n" if $DEBUG;
	foreach my $file ( @fileList ) {
		if ($file =~ /\/([0-9a-zA-Z\_\-]+?).xml$/) {
			my $csv_file = $xml_dir{$path}.$1.'.csv';
			chomp($file);
			my @data = &open_xml( $file );
			if ($#data > -1) {
				push @threads, async { \&writeToFile($csv_file, \@data); };
			}
		}
	}
}
$_->join for @threads;
close (LOG);

# ::::::::::::::::::::::::::::::::::::::::::::::::::
# open xml
#
# 0: path to file
# return: array of array of calls data(%nodes)
sub open_xml
{
	my $fileWithPath = $_[0];
	my $doc;
	my @dataArr;
	my $callCount = 0;
	
	print LOG "\t&open_xml( $fileWithPath )\n" if $DEBUG;
	# load
	my $parser = XML::LibXML->new();
    	eval { $doc    = $parser->parse_file( $fileWithPath ) };
    	if ($@) {
            print LOG "Err: Empty file $fileWithPath\n";
            return;
        }
	foreach my $call ($doc->findnodes('/Calls/Call')) {
		my @callArr;
		foreach my $child ($call->findnodes('*')) {
		       	if ( exists($nodes{$child->nodeName()}) ) {
				$callArr[$nodes{$child->nodeName()}] = $child->textContent;
			} else {
				print LOG "Warn: ". $child->nodeName()." not configured !\n";
				die "Unmatched nodeName. Please cheack log file.\n" if $DIE_ON_WARN;
			}	
		}
		push @dataArr, [ @callArr ];
		$callCount++;
	}
	print LOG "\t\t$fileWithPath\t[$callCount]\n";
	return ( @dataArr );
}
# ::::::::::::::::::::::::::::::::::::::::::::::::::
#
sub writeToFile
{	
	my $csv_file = $_[0];
	my $arr = $_[1];
	#my $start = new Benchmark;
	$threadCount->up();
	print "Number of thread: ". $threadCount->get() ."| $_[0]\n";
	open (CSV_FH, ">$csv_file") or die "Can't open file $csv_file\n";
	print CSV_FH &getCSVHeader."\n";
	foreach my $refToLine (@$arr) {
		my $line = '';
		for(my $i= 0; $i < keys( %nodes ); $i++) {

			$line .=  @{$refToLine}[$i] if defined @{$refToLine}[$i];
			$line .= $CSV_SEPARATOR;
		}
		chop($line);
		print CSV_FH $line . "\n";
	}
	close (CSV_FH);
	#my $end = new Benchmark;
	#print  "\t\t:  |". timestr(timediff($end, $start), 'all') . "\n";
	$threadCount->down();
}
# ::::::::::::::::::::::::::::::::::::::::::::::::::
# get CSV Header as string
#
# return: (string)'$nodes;$nodes;$nodes'
sub getCSVHeader
{
	my $retH;
	foreach my $H (sort { $nodes{$a}<=> $nodes{$b} } keys %nodes) {
		$retH .= $H.$CSV_SEPARATOR;
	}
	chop($retH);
	return $retH;
}
__DATA__
