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

set (array_src
    Metadata.cpp
    Array.cpp
    MemArray.cpp
    EmbeddedArray.cpp
    StreamArray.cpp
    DelegateArray.cpp
	TupleArray.cpp
    ComplementArray.cpp
    FileArray.cpp
    DBArray.cpp
    ParallelAccumulatorArray.cpp
    RLE.cpp
)

file(GLOB array_include "*.h")

add_library(array_lib STATIC ${array_src} ${array_include})
target_link_libraries(array_lib catalog_lib json_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
add_dependencies(array_lib scidb_msg_lib)

