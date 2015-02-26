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

set (catalog_src
	SystemCatalog.cpp
)

file(GLOB catalog_include "*.h")

add_library(catalog_lib STATIC ${catalog_src} ${catalog_include})

target_link_libraries(catalog_lib ${Boost_LIBRARIES})
target_link_libraries(catalog_lib pqxx)
target_link_libraries(catalog_lib util_lib)
target_link_libraries(catalog_lib ${LIBPQ_LIBRARIES})

configure_file(data/meta.sql "${GENERAL_OUTPUT_DIRECTORY}/data/meta.sql" COPYONLY)

add_subdirectory(data)

add_dependencies(catalog_lib genmeta)
