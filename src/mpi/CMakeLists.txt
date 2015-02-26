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

message(STATUS "****************** BEGIN mpi/CMakeLists.txt ******************")

if(SCALAPACK_TIMING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_TIMING")
endif()

if(SCALAPACK_DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_DEBUG")
endif()

######################
# MPI
######################
set(USE_LOCAL_MPI 0)
if (USE_LOCAL_MPI)
  set(LOCAL_MPI_PATH "/home/james/packages/ompi/openmpi-1.4.5") # example
  message(STATUS "WARNING-- using locally installed MPI")
  set(MPI_INCLUDE "-I. -I ${LOCAL_MPI_PATH}/build/ompi/include -I  ${LOCAL_MPI_PATH}/build/opal/include")
  set(MPI_Fortran_COMPILER "${LOCAL_MPI_PATH}/bin/mpif77")
  set(MPI_CXX_COMPILER "${LOCAL_MPI_PATH}/bin/mpic++")
  set(MPI_LIBRARIES "-L${LOCAL_MPI_PATH}/lib ...") # fill in the blanks
  set(MPI_CXX_FOUND 0)     #set to 1 to pick up custom values
  set(MPI_Fortran_FOUND 0) #set to 1 to pick up custom values
else (USE_LOCAL_MPI)
  set(MPI_Fortran_COMPILER mpif77) # must be before find_package to be path-resolved
  find_package(MPI)
  MESSAGE(STATUS "MPI_Fortran_COMPILER=${MPI_Fortran_COMPILER}")
endif(USE_LOCAL_MPI)

#
# DLA is built, by default, using the platform-provided set of ScaLAPACK, BLACS, LAPACK, and BLAS
# packages (called ScaLAPACK stack below, for brevity).
#
# A much faster version of those can be provided by the Intel MKL, if you have
# a license for Intel MKL on the machine which you are compiling.
# (because although the MKL binaries can be re-distributed by licensees, the
#  corresponding Intel headers may not be redistributed -- each copy requires
#  a license).
#
# So we want to be able to have the default configuration build against the platform
# version, even on build machines where the MKL is installed, but be able to change
# a setting and have the build switch to using MKL.  We want explicit control of whether
# MKL is used, as the same build machine must build regular and MKL versions without
# installing and removing the MKL component.  Instead, a configuration parameter at
# the time of the cmake command must control whether MKL building is attempted.
# Most developers will not be concerned with this option, as their development of the
# DLA plugins does not depend on the performance of the implementation of the
# ScaLAPACK stack behind it.
#
# The current implementation is to have this file check for
# USE_MKL_LP64 and USE_MKL_ILP64 parameters.
#
# USE_MKL_LP64 - the ScaLAPACK stack uses 64-bit pointers and arithmetic, but
#                the FORTRAN "INTEGER" type is 32bits.  64-bit integers are only used for the
#                "INTEGER*8" type.  This is complatible with the Ubuntu version of the
#                ScaLAPACK stack, but problem "WORK" space is limited to 2^31 values.
#
# USE_MKL_ILP64 - the ScaLPACK stack uses 64-bit pointers, arithmetic, and INTEGER types.
#                The RHEL / Centos default stack may well be using this model, TBD.
#

set(USE_FIND "true") # assume not using MKL

if(USE_MKL_LP64)
    unset(USE_FIND)
    message(STATUS "USE_MKL_LP64:  Scalapack configured for MKL")
    # The flags and paths for the LP64 model
    # below were determined with the assistance of the web applet at:
    # http://software.intel.com/en-us/articles/intel-mkl-link-line-advisor/
    # with the following inputs:
    # -------------------
    # Intel(R) MKL 10.3
    # Linux*
    # Intel(R) 64
    # GNUFortran
    # Static
    # LP64 (32-bit integer)
    # Sequential (and repeated with Parallel)
    # [check] ScaLAPACK (BLACS required)
    # [check] BLACS
    # Open MPI
    # -----------------

    set(MKLROOT "/opt/intel/mkl")
    set(MKL_INCD "${MKLROOT}/include")
    set(MKL_LIBD "${MKLROOT}/lib/intel64")
    # choose one of the following to select sequential or parallel:
    # set(MKL_PARALLEL 1) # if enabled, requires setting environement variables to limit num threads used
    if(MKL_PARALLEL)
        message(STATUS "MKL_PARALLEL:  Scalapack configured for parallel MKL BLAS")
        set(MKL_SEQ "gnu_thread") # threaded BLAS via openmp
        set(MKL_SEQ2 "-fopenmp;") #  openmp support, must end with ";"
    else()
        set(MKL_SEQ "sequential") # single-threaded BLAS OR
        set(MKL_SEQ2 "")          # nothing
    endif()

    set(SCALAPACK_LIBRARIES "${MKL_LIBD}/libmkl_scalapack_lp64.a;-Wl,--start-group;${MKL_LIBD}/libmkl_gf_lp64.a;${MKL_LIBD}/libmkl_${MKL_SEQ}.a;${MKL_LIBD}/libmkl_core.a;${MKL_LIBD}/libmkl_blacs_openmpi_lp64.a;-Wl,--end-group;${MKL_SEQ2}-lpthread;-lm")
    set(LAPACK_LIBRARIES "")
    set(BLAS_LIBRARIES "")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -m64 -I${MKL_INC_DIR}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64 -I{MKL_INC_DIR}")  # not sure if this one is needed

    set(SCALAPACK_FOUND "1")        # emulate that finds worked
    set(LAPACK_FOUND "1")
    set(BLAS_FOUND "1")
endif(USE_MKL_LP64)

if(USE_MKL_ILP64)
    unset(USE_FIND)
    message(STATUS "USE_MKL_ILP64 set -- configuring for large-memory MKL Scalapack libs")
    #######################################
    # MKL ILP64 setup: (was working at one point, not tested recently)
    #
    # Later on, in the October timeframe, when building with MKL on RHEL 5. for the purpose of generating
    # binaries for commercial customers, we will want to use the following settings, which extend
    # ScaLAPACK to use 64-bit integers to describe the size of matrix WORK areas (as
    # well as every other INTEGER type in the ScaLAPACK FORTRAN code.
    #
    set(CMAKE_Fortran_FLAGS "${CMAKE_FORTRAN_FLAGS} -DMKL_ILP64 -m64 -I${MKL_INC_DIR}")
    set(SCALAPACK_LIBRARIES "-L ${MKL_LIB_DIR} -lmkl_scalapack_ilp64 -lmkl_blacs_intelmpi_ilp64")
    set(LAPACK_LIBRARIES "-L ${MKL_LIB_DIR} -lmkl_intel_ilp64 -lmkl_sequential -lmkl_core -lmkl_rt -lmkl_def -lpthread -lm")
    set(BLAS_LIBRARIES "-L ${MKL_LIB_DIR} -lmkl_rt")
    # mkl_avx or mkl_def needed
endif(USE_MKL_ILP64)

if(USE_FIND)
    ######################
    # BLAS
    ######################
    set(USE_LOCAL_BLAS 0)
    if(USE_LOCAL_BLAS)
        message(STATUS "WARNING-- using locally installed BLAS")
        set(BLAS_LIBRARIES "-L/usr/local/lib")
        # if ATLAS BLAS, have to add:
        # set(BLAS_LIBRARIES, "${BLAS_LIBRARIES} -lptcblas -lptf77blas -latlas ")
        # set(BLAS_LIBRARIES, "${BLAS_LIBRARIES} -pthread /usr/lib/gcc/x86_64-linux-gnu/4.6.1/libgfortran.so")
        # The generic BLAS, add:
        # set(BLAS_LIBRARIES, "${BLAS_LIBRARIES} -lblas ")
        # or:
        # set(BLAS_LIBRARIES, "${BLAS_LIBRARIES} -/usr/local/lib/libblas.a ")
        set (BLAS_FOUND 0) #set to 1 to pick up local custom values
    else(USE_LOCAL_BLAS)
        message(STATUS "Using platform BLAS")
        find_package(BLAS)
    endif(USE_LOCAL_BLAS)

    if (BLAS_FOUND)
        #message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
    else(BLAS_FOUND)
        message(WARNING "BLAS not found. ScaLAPACK-based functionality will NOT be built")
	message(STATUS "Please look to doc/build.txt for list of dependencies")
    endif (BLAS_FOUND)

    ######################
    # LAPACK
    ######################
    set(USE_LOCAL_LAPACK 0)
    if(USE_LOCAL_LAPACK)
        message(STATUS "WARNING-- using locally installed LAPACK")
        set(LAPACK_LIBRARIES "-L/usr/local/lib -ltmg -lreflapack")  # DEBUG
        # or set(LAPACK_LIBRARIES "/usr/local/lib/liblapack.a")  # DEBUG
        set (LAPACK_FOUND 0) #set to 1 to pick up local custom values
    else(USE_LOCAL_LAPACK)
      message(STATUS "Using platform LAPACK")
      find_package(LAPACK)
    endif(USE_LOCAL_LAPACK)

    if (LAPACK_FOUND)
        #message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
    else(LAPACK_FOUND)
        message(WARNING "LAPACK not found. ScaLAPACK-based functionality will NOT be built")
	message(STATUS "Please look to doc/build.txt for list of dependencies")
    endif(LAPACK_FOUND)

    ###########################
    # ScaLAPACK
    ###########################
    set(USE_LOCAL_SCALAPACK 0) # 1 for using /usr/local/lib                 #DEBUG
    if(USE_LOCAL_SCALAPACK)
        message(STATUS "WARNING-- using locally installed ScaLAPACK stack")
        set(SCALAPACK_LIBRARIES "/usr/local/lib/libscalapack.a")
        set(SCALAPACK_INC_DIR   "/usr/local/include/BLACS/SRC")
        set(CMAKE_C_FLAGS       "${CMAKE_C_FLAGS} -I ${SCALAPACK_INC_DIR}")
        set(SCALAPACK_FOUND 0) #set to 1 to pick up local custom values
    else()
        message(STATUS "Using platform ScaLAPACK stack")
        find_package(ScaLAPACK)
    endif()

    if (SCALAPACK_FOUND)
        #message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
    else (SCALAPACK_FOUND)
        message(WARNING "ScaLAPACK not found. ScaLAPACK-based functionality will NOT be built.")
        message(WARNING "Ubuntu Hint: $ sudo apt-get install libscalapack-mpi1")
    endif (SCALAPACK_FOUND)
endif(USE_FIND)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOMPI_IGNORE_CXX_SEEK")

if(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    message(WARNING "MPI not present. ScaLAPACK-based functionality will NOT be built")
    message(WARNING "Debug: MPI_C_FOUND is ${MPI_C_FOUND}, MPI_CXX_FOUND is ${MPI_CXX_FOUND}, MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "Ubuntu Hint: $ sudo apt-get install libopenmpi-dev")
else(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    if(${DISTRO_NAME_VER} MATCHES "RedHat")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lgfortran")
    endif(${DISTRO_NAME_VER} MATCHES "RedHat")
endif(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)

message(STATUS "Debug: FINAL PACKAGE SETTINGS ================================>")
message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
message(STATUS "Debug: MPI_LIBRARIES is ${MPI_LIBRARIES}")
message(STATUS "Debug: MPI_INCLUDE is ${MPI_INCLUDE}")
message(STATUS "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")
message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_FLAGS is ${CMAKE_Fortran_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
message(STATUS "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
message(STATUS "Debug: FINAL PACKAGE SETTINGS <================================")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    #
    # build the common mpi framework code
    #
    set (mpi_common_src
        MPIUtils.cpp
    )
    add_library(mpi_common STATIC ${mpi_common_src})
    target_link_libraries(mpi_common util_lib)

    #
    # add the mpi test operator and scalapack-based DLA operators
    # to the mpi libraray
    #
    set (mpi_src
        MPIManager.cpp
        MPILauncher.cpp
        MPISlaveProxy.cpp
        MPIInitLogical.cpp
        MPIInitPhysical.cpp
        MPIPhysical.cpp
        test/MPITestLogical.cpp
        test/MPITestPhysical.cpp
    )

    #
    # build the mpi-slave program
    #
    add_subdirectory("slaving")

else (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)

     set (mpi_src
          pluginNoMpi.cpp
     )

    message(WARNING "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    message(WARNING "Can't build ScaLAPACK extensions")
    message(WARNING "without ScaLAPACK, Open MPI, LAPACK, or a BLAS")
    message(WARNING "or without MPI_CXX")
    message(WARNING "or without MPI_Fortran interfaces.")
    message(WARNING "This is only a warning and is not a problem unless")
    message(WARNING "you want the libdense_linear_algebra.so plugin to be built.")
    message(WARNING "")
    message(WARNING "If you intended to built it, the following will help you debug what is missing.")
    message(WARNING "-------------------------------------------------")
    message(WARNING "Debug: MPI_Fortran_COMPLIER is ${MPI_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
    message(WARNING "Debug: SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
    message(WARNING "Debug: LAPACK_FOUND is    ${LAPACK_FOUND}")
    message(WARNING "Debug: BLAS_FOUND is      ${BLAS_FOUND}")
    message(WARNING "Debug: MPI_CXX_FOUND is   ${MPI_CXX_FOUND}")
    message(WARNING "Debug: MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "----------------------------------------------------")
    message(WARNING "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

#
# complete the linar_algebra library settings
#
add_library(mpi SHARED ${mpi_src})

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)
    target_link_libraries(mpi mpi_common)
endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libmpi${CMAKE_SHARED_LIBRARY_SUFFIX}" mpi)
set_target_properties(mpi PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

if (APPLE)
    set_target_properties(mpi PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
