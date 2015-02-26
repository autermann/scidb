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
set (fits_src
    plugin.cpp
    common/FITSParser.cpp
    input/FITSInputArray.cpp
    input/LogicalFITSInput.cpp
    input/PhysicalFITSInput.cpp
    show/LogicalFITSShow.cpp
    show/PhysicalFITSShow.cpp
)

file(GLOB fits_include "*.h")

add_library(fits SHARED ${fits_src} ${fits_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libfits${CMAKE_SHARED_LIBRARY_SUFFIX}" fits)
set_target_properties(fits PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

if (APPLE)
  set_target_properties(fits PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif (APPLE)

