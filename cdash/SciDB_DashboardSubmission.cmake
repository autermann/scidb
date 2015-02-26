########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2011 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the GNU General Public License for the complete license terms.
#
# You should have received a copy of the GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
#
# END_COPYRIGHT
########################################

set(CTEST_SOURCE_DIRECTORY "$ENV{SOURCE_PATH}")
set(CTEST_BINARY_DIRECTORY "./")

SET(CTEST_CMAKE_GENERATOR "Unix Makefiles")
SET(CTEST_CMAKE_COMMAND "$ENV{EXECUTABLE_CMAKE_PATH}")
SET(CTEST_CTEST_COMMAND "$ENV{EXECUTABLE_CTEST_PATH}")
SET(CTEST_BUILD_COMMAND "/usr/bin/make -j3 install")
set(CTEST_BUILD_TARGET "install")

find_program(HOSTNAME_CMD NAMES hostname)
exec_program(${HOSTNAME_CMD} ARGS OUTPUT_VARIABLE HOSTNAME)
set(CTEST_SITE "${HOSTNAME}")
set( $ENV{LC_MESSAGES} "en_EN" )

set(CTEST_BUILD_NAME "scidb-$ENV{BUILD_TAG}")
#set(CTEST_BUILD_OPTIONS "${CTEST_BUILD_OPTIONS} -DCMAKE_BUILD_TYPE=CC -DCMAKE_INSTALL_PREFIX=/opt/$ENV{TESTMODEL}")
set(CTEST_BUILD_OPTIONS "${CTEST_BUILD_OPTIONS} -DCMAKE_BUILD_TYPE=$ENV{BUILD_TYPE} -DCMAKE_INSTALL_PREFIX=/opt/$ENV{TESTMODEL}")

find_program(CTEST_SVN_COMMAND NAMES svn)

set(WITH_COVERAGE TRUE)
find_program(CTEST_COVERAGE_COMMAND NAMES gcov)

######SET (CTEST_START_WITH_EMPTY_BINARY_DIRECTORY TRUE)

set(CTEST_UPDATE_COMMAND "${CTEST_SVN_COMMAND}")
set(CTEST_CONFIGURE_COMMAND "${CTEST_CMAKE_COMMAND} ${CTEST_BUILD_OPTIONS} ${CTEST_SOURCE_DIRECTORY}")
file(REMOVE_RECURSE ${CTEST_BINARY_DIRECTORY}/Testing/)

set (TEST_MODEL "$ENV{TESTMODEL}")
ctest_start(${TEST_MODEL})


## update
ctest_update(RETURN_VALUE updateRV)

## configure
ctest_configure(RETURN_VALUE configureRV)
if (NOT configureRV)

	## build
	ctest_build(NUMBER_ERRORS buildErrors)
	if (buildErrors EQUAL 0)

		## test
		ctest_test()

		if(EXISTS "$ENV{LOCALHOST_ROOT_DIRECTORY}/$ENV{CDASH_TESTCASES_FOLDER}")
			execute_process (COMMAND /bin/cp -R "${CTEST_BINARY_DIRECTORY}/tests/harness/testcases/r/" "$ENV{LOCALHOST_ROOT_DIRECTORY}/$ENV{CDASH_TESTCASES_FOLDER}/")
			execute_process (COMMAND /bin/cp -R "${CTEST_SOURCE_DIRECTORY}/tests/harness/testcases/t/" "$ENV{LOCALHOST_ROOT_DIRECTORY}/$ENV{CDASH_TESTCASES_FOLDER}/")
			execute_process(COMMAND /usr/bin/find . -depth \( -name *.expected -or -name *.svn -or -name *.res -or -name *.err -or -name *.aql -or -name *.afl \) -exec /bin/rm -rf {} \; -print; 
				WORKING_DIRECTORY "$ENV{LOCALHOST_ROOT_DIRECTORY}/$ENV{CDASH_TESTCASES_FOLDER}" OUTPUT_QUIET)
		endif()

		## coverage
		ctest_coverage()

		## prepare customized test report

		execute_process(COMMAND ${CTEST_SVN_COMMAND} info . OUTPUT_VARIABLE _svn_out WORKING_DIRECTORY "${CTEST_SOURCE_DIRECTORY}")
		string(REGEX REPLACE "^.*Revision: ([^\n]*).*$" "\\1" MY_REV ${_svn_out})
		set(ENV{revisionURL} ${MY_REV})

		set(GENERAL_HARNESSTEST_DIRECTORY "${CTEST_BINARY_DIRECTORY}/tests/harness")

		file(READ "${CTEST_BINARY_DIRECTORY}/Testing/TAG" tag_file)
		string(REGEX MATCH "[^\n]*" xml_dir ${tag_file})
		set(ALL_REPORTS_DIRPATH "${CTEST_BINARY_DIRECTORY}/Testing/${xml_dir}")

		set(TEST_DOT_XML_PATH "${ALL_REPORTS_DIRPATH}/Test.xml")
#		set(HARNESS_REPORT_DOT_XML "${GENERAL_HARNESSTEST_DIRECTORY}/testcases/Merged_report.xml")
		set(HARNESS_REPORT_DOT_XML "${GENERAL_HARNESSTEST_DIRECTORY}/testcases/Report.xml")
		set(SCIDB_FINALTEST_REPORT_DOT_XML "${GENERAL_HARNESSTEST_DIRECTORY}/testcases/CDASH_SciDBFinalReport.xml")
#		execute_process (COMMAND python merge_reports.py
#						 WORKING_DIRECTORY "${CTEST_SOURCE_DIRECTORY}/tests/harness")

# xxx --- ctestreport ignored? SCIDB_FINALTEST_REPORT_DOT_XML is read from env.
		execute_process (COMMAND ${GENERAL_HARNESSTEST_DIRECTORY}/preparecdashreport --ctestreport=${TEST_DOT_XML_PATH} --harnessreport=${HARNESS_REPORT_DOT_XML}
                                                 WORKING_DIRECTORY ".")

		execute_process (COMMAND /bin/cp ${SCIDB_FINALTEST_REPORT_DOT_XML} ${ALL_REPORTS_DIRPATH}/Test.xml
						 WORKING_DIRECTORY ".")
	endif()
endif()

## submit
set(CTEST_NOTES_FILES "$ENV{CDASH__LOGFILE}")
ctest_submit()
execute_process (COMMAND /bin/cp "$ENV{CDASH__LOGFILE}" "$ENV{LOCALHOST_ROOT_DIRECTORY}/$ENV{CDASH_TESTCASES_FOLDER}/r/" WORKING_DIRECTORY ".")
