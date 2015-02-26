########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2012 SciDB, Inc.
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
    # This is the way the command string should be built up, instead of with the workaround.
    #set(CMD_ARGS " --verbose")
    #set(CMD_ARGS "${CMD_ARGS} --mpibindir=/usr/bin")
    #set(CMD_ARGS "${CMD_ARGS} --mpirun=mpiexec.mpich2")
    #set(CMD_ARGS "${CMD_ARGS} --mpif90=${MPI_Fortran_COMPILER}")
    #set(CMD_ARGS "${CMD_ARGS} --mpicc=${MPI_CC_COMPILER}")
    #set(CMD_ARGS "${CMD_ARGS} --mpiincdir='${MPI_INCLUDE}'")
    #set(CMD_ARGS "${CMD_ARGS} --lapacklib='/usr/lib/liblapack.so ${BLAS_LIBRARIES}'") # ${LAPACK_LIBRARIES} breaks it, has an embedded ;
    #set(CMD_ARGS "${CMD_ARGS} --blaslib='${BLAS_LIBRARIES}'")
    #set(CMD_ARGS "${CMD_ARGS} --noopt='-fPIC -g -O0'")
    #set(CMD_ARGS "${CMD_ARGS} --ccflags='-fPIC -g -O0'") # to debug
    #set(CMD_ARGS "${CMD_ARGS} --fcflags='-fPIC -g -O0'") # to debug
    #message(STATUS "DEBUG-- CMD_ARGS=${CMD_ARGS}")

    set(UNTAR_DIR "${CMAKE_CURRENT_BINARY_DIR}/container/src/scalapackForMPICH2")
    message(STATUS "DEBUG: UNTAR_DIR=${UNTAR_DIR}")
    ExternalProject_Add(scalapackForMPICH2
                        PREFIX container
                        URL http://www.netlib.org/scalapack/scalapack_installer.tgz
                        TIMEOUT 120
                        CONFIGURE_COMMAND ""
                        BUILD_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/external_project_build_cmd_workaround.cmake.sh ${UNTAR_DIR}
                        INSTALL_COMMAND "")
                        #      but that didn't work, so had to define it
endif()
