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

add_subdirectory(proto)

if (NOT WITHOUT_SERVER)
    set (network_src
        NetworkManager.cpp
        BaseConnection.cpp
        Connection.cpp
        MessageHandleJob.cpp
        ClientMessageHandleJob.cpp
        MessageUtils.cpp
    )

    file(GLOB network_include "*.h" "proto/*.proto")

    add_library(network_lib STATIC ${network_src} ${network_include})
    target_link_libraries(network_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
    target_link_libraries(network_lib catalog_lib scidb_msg_lib io_lib compression_lib qproc_lib)
endif()
