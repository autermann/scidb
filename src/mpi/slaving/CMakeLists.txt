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
cmake_minimum_required(VERSION 2.8.5)

#
# The program in this directory is not just for mpi.
# Ideally, we would give the mpi directory its own test-slave program, but
# at the moment, SciDB can only manage one slave program.
# So for a while we will build it here, but include slave modules in it from elsewhere.
# Until that is cleaned up, we will extend the include searching to include the root
# directory of all such "elsewhere" slave modules.
# TODO: once dla-scalapack is organized and stable in SciDB, reverse this dependency.
#       and have the dla-scalapack slave include the mpi testing.  This is more legitimate
#       because that code already depends on the mpi directory tree, and the reverse is
#       not desireable (general thing: mpi depending on specific thing: dla-scalapack).
include_directories("${CMAKE_SOURCE_DIR}/src/linear_algebra")


message(STATUS "****************** BEGIN mpi/slaving/CMakeLists.txt ******************")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MPI_INCLUDE}")
    set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})         # hack - we don't understand FindMPI yet
    set(CMAKE_Fortran_COMPILER ${MPI_Fortran_COMPILER}) # hack - we don't understand FindMPI yet

    message(STATUS "Debug: CMAKE_CXX_COMPLIER is ${CMAKE_CXX_COMPILER}")
    message(STATUS "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
    message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")

    #
    # build the mpi-slave program
    #
    set (scidb_mpi_slave_src
         mpi_slave.cpp
    #    NOTE: the following items are to be moved back into their
    #    own slave in the dla area
         ../../linear_algebra/scalapackUtil/test/slaving/mpiCopySlave.cpp
         ../../linear_algebra/scalapackUtil/test/slaving/mpiRankSlave.cpp
         ../../linear_algebra/dlaScaLA/slaving/pdgemmSlave.cpp
         ../../linear_algebra/dlaScaLA/slaving/pdgesvdSlave.cpp
         ../../linear_algebra/dlaScaLA/slaving/slaveTools.cpp
        ../../linear_algebra/scalapackUtil/reformat.cpp
    )

    add_executable(scidb_mpi_slave ${scidb_mpi_slave_src})
    
    # Temporary until separate dla-scalapack slave
    target_link_libraries(scidb_mpi_slave ${SCALAPACK_LIBRARIES})
    target_link_libraries(scidb_mpi_slave ${LAPACK_LIBRARIES})
    target_link_libraries(scidb_mpi_slave ${BLAS_LIBRARIES})
    target_link_libraries(scidb_mpi_slave ${MPI_LIBRARIES})
    target_link_libraries(scidb_mpi_slave  mpi_common
                                           scidbclient
                                           scidb_msg_lib
                                           ${LOG4CXX_LIBRARIES}
                                           ${PROTOBUF_LIBRARY}
                                           ${LIBRT_LIBRARIES}
                                           ${Boost_LIBRARIES})
    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "scidb_mpi_slave" scidb_mpi_slave)
    set_target_properties(scidb_mpi_slave PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)
