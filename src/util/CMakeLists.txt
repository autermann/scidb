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

set(SAMPLE_CONFIG_IN "${CMAKE_CURRENT_SOURCE_DIR}/scidb-sample.conf.in")
set(SAMPLE_CONFIG "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb-sample.conf")
message(STATUS "Configuring ${SAMPLE_CONFIG_IN} as ${SAMPLE_CONFIG}")
configure_file(${SAMPLE_CONFIG_IN} ${SAMPLE_CONFIG} @ONLY)

set (util_src
    Job.cpp
    JobQueue.cpp
    Semaphore.cpp
    Thread.cpp
    ThreadPool.cpp
    PluginManager.cpp
    FileIO.cpp
    PluginObjects.cpp
    na.cpp
    naInternal.cpp
    require.cpp
    InjectedError.cpp
    BufferedFileInput.cpp
    shm/SharedMemoryIpc.cpp
)

file(GLOB util_include "*.h")

configure_file(log4cxx.properties "${GENERAL_OUTPUT_DIRECTORY}/log4cxx.properties" COPYONLY)

add_library(util_lib STATIC ${util_src} ${util_include})
target_link_libraries(util_lib json_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES} ${LIBRT_LIBRARIES})
