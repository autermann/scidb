#!/usr/bin/perl
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#    
# This script takes as input a directory name (root of either SciDB trunk or P4 trunk),
# and can replaces all the occurrences of copyright blocks with the content in 
# license.txt (if the copyright block starts with "*") or license.py.txt (if the block starts with "#"),
# and add missing copyright blocks.
# 
# @note The following files are omitted:
#   - license.txt
#   - license.py.txt
#   - *~
#   - FindProtobuf.cmake (See #3215)
#
# @note The following directories are omitted:
#   - .svn
#   - 3rdparty
#   - extern
#
# @note The files in which missing copyright blocks are added are 
#   - py, h, c, cpp, hpp, sh, java, yy, ll
#   - their "in" version, e.g. py.in.
#   To expand the script to handle more file types, follow the example of 'java' (search all occurences of 'java' in this file).
#
# @def A "copyright block" is a block of lines starting with a line that includes BEGIN_COPYRIGHT and ends with a line that includes END_COPYRIGHT.
#
# @author dzhang
#
$beginCopyright = "BEGIN_COPYRIGHT";
$endCopyright = "END_COPYRIGHT";
$newCopyrightFileStar = "license.txt";
$newCopyrightFileSharp = "license.py.txt";
%hashSuffixToCount;         # has copyright
%hashSuffixToCountMissing;  # no copyright
$newCopyrightBlockStar;
$newCopyrightBlockSharp;

# As the name says.
#
sub printUsageAndExit
{
	print "Usage: ", __FILE__, " rootDir [replace]\n";
	print "In the read mode, the script shows how many files contains the copyright block, breaking down by suffix.\n";
	print "In the replace mode, the script replaces the copyright blocks with new content, or add if not exist.\n";
	exit;
}

# Return the first char of the line containing $beginCopyright, if found; or 0, if not found.
# @param file
# @return the first char of the copyright line, if copyright block is found; or 0.
# @throw if the first chars of the lines containing $beginCopyright and $endCopyright do not match.
# @throw if the first char of the line containing $beginCopyright is not "*" or "#".
# @throw if there exists a line containing $beginCopyright but not a line containing $endCopyright.
#
sub returnFirstCharIfMatch
{
	my($file) = @_;
	local(*HAND);
	my $firstChar;  # first char of the beginCopyright line; or false if not exist
	
	open(HAND, $file) or die "Cannot open $file.\n";
	while (<HAND>) {
		if ( /^\s*(.)\s+$beginCopyright/ ) {
			$firstChar = $1;
			die "First char not * or #!\n" unless ($firstChar eq "*" or $firstChar eq "#");
			last;
		}
	}

	unless ($firstChar) {
		close HAND;
		return 0;
	}

	while (<HAND>) {
		if ( /^\s*(.)\s+$endCopyright/ ) {
			unless ($firstChar eq $1) {
				die "ERROR! In $file, the lines of $beginCopyRight and $endCopyright have different first char.\n";
			}
			close HAND;
			return $firstChar;
		}
	}
	die "ERROR! $file has $beginCopyright but not $endCopyright.\n";
}

# Get the suffix of a file name.
# @param[in] filename
# @return the suffix
#
# @note: certain types of files with suffix "in" have more detailed suffixs returned. 
#   E.g. "py.in" is a different category from "in".
#   These are the types given at the top of the file, for which missing copyright blocks will be added, 
#
sub getSuffix
{
	my($file) = @_;
	if ($file =~ /(.*)\.((py|h|c|cpp|hpp|sh|java|yy|ll)\.in)$/) {
		return $2;
	}
	elsif ($file =~ /(.*)\.(.+)/) {
		return $2;
	}
	return $file;
}

# Replace the copyright block in a file with a new copyright block, either $newCopyrightStar or $newCopyrightSharp.
# @param file
# @param isStar  whether using $newCopyrightFileStar
#
sub replace
{
	# sanity check
	die "calling replace(), but new copyright not set" unless ($newCopyrightStar and $newCopyrightSharp);

	my $tmpFile = "/tmp/replace_copyright_tmp.txt";
	my($file, $isStar) = @_;
	my $state = 0; # 0: before copyright; 1: during copyright; 2: after copyright

	# copy to preserve the permission
	system("cp", "$file", "$tmpFile");
	
	local(*HAND2);
	open(HAND2, ">$tmpFile") or die "Can't write to $tmpFile.";

	local(*HAND);
	open(HAND, $file) or die "Can't open $file for read.\n";
	while (<HAND>) {
		if ($state == 0) {
			if ( /^\s*(.)\s+$beginCopyright/ ) {
				if ($isStar) {
					die "$_, $file, $isStar, $1" unless ($1 eq "*");
				}
				else {
					die unless ($1 eq "#");
				}
				$state = 1;
			}
			else {
				print HAND2;
			}
		}
		elsif ($state == 1) {
			if ( /^\s*(.)\s+$endCopyright/ ) {
				if ($isStar) {
					print HAND2 $newCopyrightStar;
				}
				else {
					print HAND2 $newCopyrightSharp;
				}
				$state = 2;
			}
		}
		else {
			print HAND2;
		}
	}
	close HAND2;
	close HAND;

	system("mv", "$tmpFile", "$file");
}

# Add the copyright block in a file.
# @param file
# @param isStar  whether using $newCopyrightFileStar
#
sub add
{
	# sanity check
	die "calling add(), but new copyright not set" unless ($newCopyrightStar and $newCopyrightSharp);

	my $tmpFile = "/tmp/replace_copyright_tmp.txt";
	my($file, $isStar) = @_;
	
	# copy to preserve the permission
	system("cp", "$file", "$tmpFile");

	local(*HAND2);
	open(HAND2, ">$tmpFile") or die "Can't write to $tmpFile.";

	local(*HAND);
	open(HAND, $file) or die "Can't open $file for read.\n";

	if ($isStar) {
		print HAND2 "/*\n";
		print HAND2 "**\n";
		print HAND2 $newCopyrightStar;
		print HAND2 "*/\n";
	} else {
		# For shells, typically the first line starts with "#!". Keep it that way.
		my $firstLine = <HAND>;
		my $firstLinePrinted = 0;
		if ($firstLine and $firstLine =~ /^#!/) {
			print HAND2 $firstLine;
			$firstLinePrinted = 1;
		}

		print HAND2 "#\n";
		print HAND2 $newCopyrightSharp;
		print HAND2 "#\n";

		unless ($firstLinePrinted) {
			print HAND2 $firstLine;
		}
	}		

	while (<HAND>) {
		print HAND2;
	}
	close HAND2;
	close HAND;

	system("mv", "$tmpFile", "$file");
}

# For each file in the directory containing $beginCopyright, increase the count for entry in %hashSuffixToCount.
# Also, in replace mode, replace the copyright block.
# @param dir
# @param isReplaceMode
#
sub loopDir
{
	my($dir, $isReplaceMode) = @_;
	chdir($dir) || die "Cannot chdir to $dir.\n";
	local(*DIR);
	opendir(DIR, ".");
	while ($f = readdir(DIR)) {
		next if ($f eq "." || $f eq ".." || -l $f );
		if (-d $f) {
			next if ($f eq ".svn" || $f eq "3rdparty" || $f eq "extern");
			loopDir($f, $isReplaceMode);
		}
		elsif (-f $f) {
			next if ($f eq $newCopyrightFileStar || $f eq $newCopyrightFileSharp || $f =~ /\~$/ || $f eq "FindProtobuf.cmake");
			my $firstChar = &returnFirstCharIfMatch($f);
			my $suffix = &getSuffix($f);
			if ($firstChar) {
				if ($isReplaceMode) {
					&replace($f, $firstChar eq "*");
				}
				if (exists $hashSuffixToCount{$suffix}) {
					$hashSuffixToCount{$suffix} ++;
				}
				else {
					$hashSuffixToCount{$suffix} = 1;
				}
			}
			else {
				my $missing = 0;

				# h, c, cpp, hpp, java, yy, ll
				if ($suffix eq "h"    or $suffix eq "c"    or $suffix eq "cpp"    or $suffix eq "hpp"    or $suffix eq "java"    or $suffix eq "yy"    or $suffix eq "ll" or
				    $suffix eq "h.in" or $suffix eq "c.in" or $suffix eq "cpp.in" or $suffix eq "hpp.in" or $suffix eq "java.in" or $suffix eq "yy.in" or $suffix eq "ll.in") {
					if ($isReplaceMode) {
						&add($f, 1);  # isStar
					}
					$missing = 1;
				}
				# py, sh
				elsif ($suffix eq "py"    or $suffix eq "sh" or
				       $suffix eq "py.in" or $suffix eq "sh.in") {
					if ($isReplaceMode) {
						&add($f, 0);  # not isStar
					}
					$missing = 1;
				}

				if ($missing) {
					if (exists $hashSuffixToCountMissing{$suffix}) {
						$hashSuffixToCountMissing{$suffix} ++;
					}
					else {
						$hashSuffixToCountMissing{$suffix} = 1;
					}
				}
			}
		}
	}
	closedir(DIR);
	chdir("..");
}

# Report result from %hashSuffixToCount.
#
sub report
{
	my $count = 0, $countMissing = 0;
	print "******** Has copyright *********\n";
	foreach my $key (keys %hashSuffixToCount) {
		print $key, "\t", $hashSuffixToCount{$key}, "\n";
		$count += $hashSuffixToCount{$key};
	}
	print "Subtotal: $count.\n";

	print "******** No copyright *********\n";
	foreach my $key (keys %hashSuffixToCountMissing) {
		print $key, "\t", $hashSuffixToCountMissing{$key}, "\n";
		$countMissing += $hashSuffixToCountMissing{$key};
	}
	print "Subtotal: $countMissing.\n";

	print "******** Total ****************\n";
	print "Total match: " . ($count + $countMissing) . ".\n";
}

# Get the new copyright.
# @param isStar
# @return the new copyright block
#
sub getNewCopyright
{
	my($isStar) = @_;
	my $copyright;
	my $firstChar = $isStar ? "*" : "#";
	my $file = $isStar ? $newCopyrightFileStar : $newCopyrightFileSharp;

	local(*HAND);
	my $state = 0;  # 0: before copyright; 1: during copyright; 2: after copyright

	open(HAND, $file) or die "Can't open $file for read.\n";
	while (<HAND>) {
		if ($state == 0) {
			if ( /^\s*(.)\s+$beginCopyright/ ) {
				$copyright .= $_;
				$state = 1;
			}
		}
		elsif ($state == 1) {
			$copyright .= $_;
			if ( /^\s*(.)\s+$endCopyright/ ) {
				$state = 2;
			}
		}
	}
	close HAND;

	# sanity check
	my $c = &returnFirstCharIfMatch($file);
	die "$file does not start with $firstChar" unless ($c and ($c eq $firstChar));
	return $copyright;
}

# The main function.
#
sub main
{
	if ($#ARGV < 0 || $#ARGV > 1 || ($#ARGV==1 && not $ARGV[1] eq "replace")) {
		printUsageAndExit;
	}
	$newCopyrightStar = &getNewCopyright(1); # true = isStar
	$newCopyrightSharp = &getNewCopyright(0); # false = isSharp
	my $root = $ARGV[0];
	my $isReplaceMode = ($#ARGV==1);
	if ($isReplaceMode) {
		print "Replacing copyright blocks in '$root' ...\n";
	}
	else {
		print "Examining (read-only) copyright blocks in '$root' ...\n";
	}
	&loopDir($root, $isReplaceMode);
	&report;
}

&main;

