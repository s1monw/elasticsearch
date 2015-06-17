#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use Digest::SHA qw(sha1);
use FindBin;
chdir "$FindBin::RealBin/../" or die $!;

my $mode = shift(@ARGV) || die usage();
my $dir  = shift(@ARGV) || die usage();
$dir=~s{/$}{};

our $JARS_DIR    = "$dir/target/lib/";
our $LICENSE_DIR = "$dir/licenses/";

$mode eq '--check'        ? check_shas_and_licenses($dir)
    : $mode eq '--update' ? write_shas($dir)
    :                       die usage();

#===================================
sub check_shas_and_licenses {
#===================================
    my %new      = get_shas();
    my %old      = get_sha_files();
    my %licenses = get_licenses();
    my $error    = 0;

    for my $jar ( sort keys %new ) {
        my $old_sha = delete $old{$jar};
        unless ($old_sha) {
            say STDERR "$jar: SHA is missing";
            $error++;
            next;
        }

        unless ( $old_sha eq $new{$jar} ) {
            say STDERR "$jar: SHA has changed";
            $error++;
            next;
        }

        my $license_found;
        my $license = $jar;
        $license =~ s/\.sha1//;

        while ( $license =~ s/-[^\-]+$// ) {
            if ( $licenses{$license} ) {
                $license_found = 1;
                last;
            }
        }
        unless ($license_found) {
            say STDERR "$jar: LICENSE is missing";
            $error++;
        }
    }

    if ( keys %old ) {
        say STDERR "Extra SHA files present for: " . join ", ", sort keys %old;
        $error++;
    }
    exit $error;
}

#===================================
sub write_shas {
#===================================
    my %new = get_shas();
    my %old = get_sha_files();

    for my $jar ( sort keys %new ) {
        if ( $old{$jar} ) {
            next if $old{$jar} eq $new{$jar};
            say "Updating $jar";
        }
        else {
            say "Adding $jar";
        }
        open my $fh, '>', $LICENSE_DIR . $jar or die $!;
        say $fh $new{$jar} or die $!;
        close $fh or die $!;
    }
    continue {
        delete $old{$jar};
    }

    for my $jar ( sort keys %old ) {
        say "Deleting $jar";
        unlink $LICENSE_DIR . $jar or die $!;
    }
}

#===================================
sub get_licenses {
#===================================
    my %licenses;
    for my $file ( grep {-f} glob("$LICENSE_DIR/*LICENSE*") ) {
        my ($license) = ( $file =~ m{([^/]+)-LICENSE.*$} );
        $licenses{$license} = 1;
    }
    return %licenses;
}

#===================================
sub get_sha_files {
#===================================
    my %shas;

    die "Missing directory: $LICENSE_DIR\n"
        unless -d $LICENSE_DIR;

    for my $file ( grep {-f} glob("$LICENSE_DIR/*.sha1") ) {
        my ($jar) = ( $file =~ m{([^/]+)$} );
        open my $fh, '<', $file or die $!;
        my $sha = <$fh>;
        $sha ||= '';
        chomp $sha;
        $shas{$jar} = $sha;
    }
    return %shas;
}

#===================================
sub get_shas {
#===================================
    die "Missing directory: $JARS_DIR\n"
        . "Please run: mvn clean package -DskipTests\n"
        unless -d $JARS_DIR;

    my $sha_list = `shasum $JARS_DIR/*`;

    my %shas;
    while ( $sha_list =~ /^(\w{40}) \s+ .*?([^\/]+\.jar)\s*$/xgm ) {
        $shas{"${2}.sha1"} = $1;
    }
    return %shas;
}

#===================================
sub usage {
#===================================
    return <<"USAGE";

USAGE:

    $0 --check  dir   # check the sha1 and LICENSE files for each jar
    $0 --update dir   # update the sha1 files for each jar

The <dir> can be set to e.g. 'core' or 'plugins/analysis-icu/'

USAGE

}
