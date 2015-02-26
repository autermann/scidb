########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2013 SciDB, Inc.
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
########################################

find_program(CTEST_COVERAGE_COMMAND NAMES gcov)
find_program(HOSTNAME_CMD NAMES hostname)

exec_program(${HOSTNAME_CMD} ARGS OUTPUT_VARIABLE HOSTNAME)

set(CTEST_CONFIGURE_COMMAND "/usr/bin/cmake -DCMAKE_BUILD_TYPE=$ENV{BUILD_TYPE} $ENV{SOURCE_PATH}")
set(CTEST_BUILD_COMMAND     "/usr/bin/make -j$ENV{THREAD_COUNT}")

set(CTEST_SOURCE_DIRECTORY "$ENV{SOURCE_PATH}")
set(CTEST_BINARY_DIRECTORY "$ENV{BUILD_PATH}")
set(HARNESS_PATH "$ENV{BUILD_PATH}/tests/harness")
set(TESTCASES_PATH "${HARNESS_PATH}/testcases")
set(CTEST_BUILD_NAME "scidb-$ENV{BUILD_TAG}")
set(TEST_MODEL "$ENV{TEST_MODEL}")
set(CTEST_SITE "${HOSTNAME}.local.paradigm4.com")
set(ENV{revisionURL} $ENV{BUILD_REVISION})
set(WITH_COVERAGE TRUE)
file(REMOVE_RECURSE $ENV{BUILD_PATH}/Testing/)

ctest_start(${TEST_MODEL})

# configure
ctest_configure(RETURN_VALUE configureRV)
if (configureRV)
  MESSAGE(FATAL "Configure step failed")
endif (configureRV)

# build 
ctest_build(NUMBER_ERRORS buildErrors)
if (NOT (buildErrors EQUAL 0))
  MESSAGE(FATAL "Build failed")
endif(NOT (buildErrors EQUAL 0))

# test
ctest_test()

# copy result
if(EXISTS "$ENV{CDASH_PATH_RESULT}")
  execute_process (COMMAND /bin/cp -R "${HARNESS_PATH}/testcases/r/" "$ENV{CDASH_PATH_RESULT}/")

  # remove un-needed files
  execute_process(COMMAND /usr/bin/find . -depth \( -name *.expected -or -name *.svn \) -exec /bin/rm -rf {} \; 
  WORKING_DIRECTORY "$ENV{CDASH_PATH_RESULT}/../" OUTPUT_QUIET)

  # remove build results older than 30 days
  execute_process(COMMAND /usr/bin/find . -maxdepth 1 -mtime +30 -type d -exec /bin/rm -rf {} \; 
  WORKING_DIRECTORY "$ENV{CDASH_PATH_RESULT}/../" OUTPUT_QUIET)

endif()

# coverage
ctest_coverage()

# prepare test report
file(READ "$ENV{BUILD_PATH}/Testing/TAG" tag_file)
string(REGEX MATCH "[^\n]*" xml_dir ${tag_file})

set(ALL_REPORTS_DIRPATH "$ENV{BUILD_PATH}/Testing/${xml_dir}")
set(TEST_DOT_XML_PATH "${ALL_REPORTS_DIRPATH}/Test.xml")
set(HARNESS_REPORT_DOT_XML "${HARNESS_PATH}/testcases/Report.xml")
set(SCIDB_FINALTEST_REPORT_DOT_XML "${HARNESS_PATH}/testcases/CDASH_SciDBFinalReport.xml")

execute_process (COMMAND ${HARNESS_PATH}/preparecdashreport --ctestreport=${TEST_DOT_XML_PATH} --harnessreport=${HARNESS_REPORT_DOT_XML} WORKING_DIRECTORY ".")
execute_process (COMMAND /bin/cp ${SCIDB_FINALTEST_REPORT_DOT_XML} ${ALL_REPORTS_DIRPATH}/Test.xml WORKING_DIRECTORY ".")

## submit
set(CTEST_NOTES_FILES "$ENV{CDASH_LOG}")
ctest_submit()
