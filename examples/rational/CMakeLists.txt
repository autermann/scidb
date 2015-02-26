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

# shared library for user defined objects
set (rational_src
    rational.cpp
)

file(GLOB rational_include "*.h")

add_library(rational SHARED ${rational_src} ${rational_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "librational${CMAKE_SHARED_LIBRARY_SUFFIX}" rational)
set_target_properties(rational PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
if (APPLE)
  set_target_properties(rational PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif (APPLE)
