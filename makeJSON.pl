#! /usr/bin/perl -w

use strict;

use JSON::XS;
use DBI;
use Data::Dumper;

my $dbh;
eval {
	$dbh = DBI->connect("dbi:SQLite:20101203.water.db");
};
if ($@) {
	die $@;
}


my $rivers = [
	{name => "Sacramento River", length => 447, flowdirection => "S", patterns => ['SACRAMENTO R ']},
	{name => "San Joaquin River", length => 330, flowdirection => "N", patterns => ['SAN JOAQUIN R ']},
	{name => "Klamath River", length => 263, flowdirection => "SW", patterns => ['KLAMATH R ']},
	{name => "Russian River", length => 110, flowdirection => "S", patterns => ['RUSSIAN R ']},
	{name => "Tuolumne River", length => 150, flowdirection => "W", patterns => ['TUOLUMNE R ']},
	{name => "Merced River", length => 112, flowdirection => "W", patterns => ['MERCED R ']},
	{name => "Trinity River", length => 130, flowdirection => "S", patterns => ['TRINITY R ']},
	{name => "Truckee River", length => 120, flowdirection => "N", patterns => ['TRUCKEE R ']},
	{name => "Colorado River", length => 122, flowdirection => "S",	patterns => ['COLORADO RIVER ']}, 
];

my $metrics = $dbh->selectall_arrayref("
	select * from metric 
	where 
--	(
--		dateTime = '2010-10-16T00:00:00.000-07:00'
--		       strftime('%Y-%m', dateTime) = '2010-09' 
--		or	   strftime('%Y-%m-%d', dateTime) = '2010-10-15'
--		or	   strftime('%Y-%m-%d', dateTime) = '2010-10-14'
--	) and 
	valueType = '00060' and 
	value >= 0
	;",
##	[ qw(siteCode valueType value dateTime) ], 
	);

my $site_name_sql_pattern = "";
foreach (@$rivers) {
	foreach (@{$_->{patterns}}) {
		$site_name_sql_pattern .= " siteName like \'\%$_\%\' or";
	}
}
# Remove the final "or"
chop($site_name_sql_pattern);
chop($site_name_sql_pattern);

my $sql_for_sites = <<"END";
	select * from site
	where $site_name_sql_pattern
	group by siteCode;
END

my $sites = $dbh->selectall_arrayref($sql_for_sites);
##	[ qw(siteCode siteName latitude longtitude) ], 

my %peakStreamflowPerSite;

my $metricsHash = {};
my $dateHash = {};
foreach  my $r (@$metrics) {
	my ($siteCode, $valueType, $value, $date) = ($r->[0], $r->[1], $r->[2], $r->[3]);
	$date =~ s/(.*?)T.*/$1/;
	if (not defined $dateHash->{$date}) { $dateHash->{$date} = scalar (keys %{$dateHash}); }
	$metricsHash->{$r->[0]}->{$dateHash->{$date}}->{$r->[1]} = $r->[2];
	
	# track the peak streamflow across all dates for any given site
	if ($valueType == "00060") {
		if (!defined $peakStreamflowPerSite{$siteCode} ||
        $value > $peakStreamflowPerSite{$siteCode}) {
			$peakStreamflowPerSite{$siteCode} = $value;
		}
	}
}

# find peakStreamflowPerSite by river now
foreach my $site ( @$sites ) {
	my ($code, $name) = ($site->[0], $site->[1]); 

	for (my $i=0; $i < scalar @$rivers; $i++) {
    foreach my $p (@{$rivers->[$i]->{patterns}}) {
      if ($name =~ m/$p/) {
        if (defined $peakStreamflowPerSite{$code} &&
            (!defined $rivers->[$i]->{peaksf} ||
             $peakStreamflowPerSite{$code} > $rivers->[$i]->{peaksf})) {
          $rivers->[$i]->{peaksf} =
            $peakStreamflowPerSite{$code};
          #remove before dump
          delete $peakStreamflowPerSite{$code}; 
        }
      }
    }
  }
} 

# Rank rivers by peakStreamflow and make rank a field
my @sorted_rivers = sort { $b->{peaksf} <=> $a->{peaksf} } 
                    @$rivers;
for (my $i=0; $i < scalar @sorted_rivers; $i++) {
  $sorted_rivers[$i]->{rank} = $i+1;
}

# Date indexes coming out of DB might not be in order. Generate an
# ordered list by sorting at the end.
my @sorted_dates = [];
my @sorted_keys = sort keys %$dateHash;
for (my $i=0; $i < scalar @sorted_keys; $i++) {
	$sorted_dates[$i] = { ymd => $sorted_keys[$i],
                        index => $dateHash->{$sorted_keys[$i]}
                      }
}

my $sql_for_reservoirs = <<"END";
	select id, latitude, longitude, capacity, river_basin, lake_name, year_fill	
  from reservoir
  ;
END

my $reservoirs = $dbh->selectall_arrayref($sql_for_reservoirs);
##	[ qw(id, latitude, longitude, capacity, river_basin, lake_name, year_fill) ], 
foreach my $r (@$reservoirs) {
	my ($cap) = ($r->[3]);
  $cap =~ s/ af//;
  $r->[3] = $cap;
}

my $sql_for_density = <<"END";
  select year, name, pop_density, pop 
  from CA_pop_density, CA_county 
  where CA_county.fips = CA_pop_density.fips
  order by year
  ;
END

my $counties = {};
my $yearHash = {};
my $popHash = {};
my $pop_density = $dbh->selectall_arrayref($sql_for_density); 
foreach my $r (@$pop_density) {
	my ($year, $name, $density, $pop) = ($r->[0], $r->[1], $r->[2],$r->[3]);
	if (not defined $yearHash->{$year}) {
    $yearHash->{$year} = scalar (keys %{$yearHash});
  }
  $pop = commify($pop);
	$popHash->{$year}->{$name} = [sprintf("%.2f", $density), $pop];
	$counties->{$name} = 1;
}

# Zero fill counties with no data
foreach my $y (keys %$yearHash) {
  foreach my $c (keys %$counties) {
    if (!defined $popHash->{$y}->{$c}) {
      	$popHash->{$y}->{$c} = [0, 0];
    }
  }
}

my @sorted_decades = sort keys %$yearHash;

open DATAJS, "> data.js" or
	die "could not open data.js";

my $coder = JSON::XS->new->ascii->pretty->allow_nonref;
print DATAJS "var rivers = ", $coder->encode (\@sorted_rivers), ";\n\n";
print DATAJS "var reservoirs = ", $coder->encode ($reservoirs), ";\n\n";
print DATAJS "var dates = ", $coder->encode (\@sorted_dates), ";\n\n";
print DATAJS "var metrics = ", $coder->encode ($metricsHash), ";\n\n";
print DATAJS "var population = ", $coder->encode ($popHash), ";\n\n";
print DATAJS "var decades = ", $coder->encode (\@sorted_decades), ";\n\n";
print DATAJS "var sites = ", $coder->encode ($sites), ";\n";

close DATAJS or
	die "could not close DATAJS";


sub commify { 
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}
