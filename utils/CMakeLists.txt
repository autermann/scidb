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

# Add subfolder of every utility
add_subdirectory("iquery")

set(CONFIGURE_PREPARE_DB_METADATA_LOCAL "data/meta.sql")

set(CONFIGURE_PREPARE_DB_METADATA "${CMAKE_INSTALL_PREFIX}/share/scidb/meta.sql")

set(CONFIGURE_SCIDBPY_CONFIG "${CMAKE_INSTALL_PREFIX}/etc/config.ini")

configure_file(scidb-prepare-db.sh.in "${GENERAL_OUTPUT_DIRECTORY}/scidb-prepare-db.sh")
configure_file(init-db.sh "${GENERAL_OUTPUT_DIRECTORY}/init-db.sh" COPYONLY)
configure_file(log1.properties "${GENERAL_OUTPUT_DIRECTORY}/log1.properties" COPYONLY)
configure_file(scidb.py.in "${GENERAL_OUTPUT_DIRECTORY}/scidb.py")
configure_file(env_setup.sh "${GENERAL_OUTPUT_DIRECTORY}/env_setup.sh" COPYONLY)
configure_file(sample_config.ini.in "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/sample_config.ini")
configure_file(config.ini.planet "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/config.ini.planet")
configure_file(config.ini.ec2 "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/config.ini.ec2")
configure_file(scidb_cores "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb_cores")
configure_file(loadcsv.py.in "${GENERAL_OUTPUT_DIRECTORY}/loadcsv.py")

add_executable(benchGen benchGen.cc) 
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "benchGen" benchGen)
set_target_properties(benchGen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

add_executable(csv2scidb csv2scidb.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "csv2scidb" csv2scidb)
target_link_libraries(csv2scidb system_lib)
set_target_properties(csv2scidb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

add_executable(scidbconf scidbconf.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "scidbconf" scidbconf)
target_link_libraries(scidbconf system_lib)
set_target_properties(scidbconf PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

configure_file(scidbLoadCsv.sh "${GENERAL_OUTPUT_DIRECTORY}/scidbLoadCsv.sh" COPYONLY)

add_executable(splitcsv splitcsv.c)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "splitcsv" splitcsv)
set_target_properties(splitcsv PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})