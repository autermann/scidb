########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
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

include(ExternalProject)

message(STATUS "****************** BEGIN mpi/local_scalapack/CMakeLists.txt ******************")

message(STATUS "DEBUG BLAS_FOUND=${BLAS_FOUND} LAPACK_FOUND=${LAPACK_FOUND} MPICH_FOUND=${MPICH_FOUND}")
if(BLAS_FOUND AND LAPACK_FOUND AND MPICH_FOUND)
    #
    # we abbreviate SCALAPACK_INSTALLER as SLPKI at times below
    #
    message(STATUS "WARNING-- using locally built libscalapack.a")
    # TODO: scalapack_installer will get the latest scalapack, not 2.0.2, need to tell installer which to fetch
    # TODO: tried to issue setup.py command directly from here, but cmake complained that it required too many
    #       arguments.  So until we can resolve that, we will use a script called
    #       external_project_build_cmd_workaround.cmake.sh
    #       WARNING: this is not a good fix, because it does not receive 5 cmake-determined
    #                configuration parameters, but since cmake doesn't like passing arguments to such programs...
    #

    string(REPLACE ";" " "  LOCAL_LAPACK_LIBRARIES "${LAPACK_LIBRARIES}")    # ${LAPACK_LIBRARIES} has an embedded ;
    set(UNTAR_DIR "${CMAKE_CURRENT_BINARY_DIR}/container/src/scalapackForMPICH2")

    message(STATUS "DEBUG-- UNTAR_DIR=${UNTAR_DIR}")
    message(STATUS "DEBUG-- MPICH2_SUFFIX=${MPICH2_SUFFIX}")
    message(STATUS "DEBUG-- MPI_INCLUDE=${MPI_INCLUDE}")
    message(STATUS "DEBUG-- BLAS_LIBRARIES=${BLAS_LIBRARIES}")
    message(STATUS "DEBUG-- LOCAL_LAPACK_LIBRARIES=${LOCAL_LAPACK_LIBRARIES}")
    message(STATUS "DEBUG-- LOCAL_MPI_PATH=${LOCAL_MPI_PATH}")

    ExternalProject_Add(scalapackForMPICH2
                        PREFIX container
                        URL http://www.netlib.org/scalapack/scalapack_installer.tgz
                        TIMEOUT 120
                        CONFIGURE_COMMAND ""
                        BUILD_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/external_project_build_cmd_workaround.cmake.sh ${UNTAR_DIR} ${MPI_INCLUDE} ${LOCAL_MPI_PATH}/bin ${LOCAL_LAPACK_LIBRARIES} ${BLAS_LIBRARIES} ${MPICH2_SUFFIX}
                        INSTALL_COMMAND "")
                        #      but that didn't work, so had to define it
endif()
