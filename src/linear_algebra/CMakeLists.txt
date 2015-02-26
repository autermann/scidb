########################################
# BEGIN_COPYRIGHT
#
# SciDB.org
#
# Copyright © 2010 - 2012 Paradigm4 Inc.
# Copyright © 2012 Scidb.org
# All Rights Reserved.
#
# END_COPYRIGHT
########################################
cmake_minimum_required(VERSION 2.8.5)

message(STATUS "*****BEGIN linear_algebra/CMakeLists.txt ***********************")

#
# The library in this directory is just for dense linear algebra
# based on scalapack.  The files form dlaScaLA should be moved up into
# here.  To protect us from constant re-writing of includes, we will
# add to the default include paths
#
include_directories("${CMAKE_SOURCE_DIR}/src/mpi")
include_directories("${CMAKE_SOURCE_DIR}/src/linear_algebra")  # include our own root

# the above is necessary to keep compatibility with the use of code in here within the
# "mpi" directory owning the "only" slave, but needing it to be build-able from here too,
# because that is where it will be shortly.

# TODO: JHM .... rename scalapackUtil to scalapack
# TODO: JHM .... move all dlaScaLA files to this "linear_algebra" directory

if(SCALAPACK_TIMING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_TIMING")
endif()

if(SCALAPACK_DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_DEBUG")
endif()

set(MPI_Fortran_COMPILER mpif77)
find_package(MPI)

if (USE_LOCAL_MPI)
  set(MPI_INCLUDE "-I. -I /home/james/packages/ompi/openmpi-1.4.5/build/ompi/include -I /home/james/packages/ompi/openmpi-1.4.5/build/opal/include")
else (USE_LOCAL_MPI)
  MESSAGE(STATUS "MPI_Fortran_COMPILER=${MPI_Fortran_COMPILER}")
endif(USE_LOCAL_MPI)

# shared library for user defined objects
#set (dense_linear_algebra_src
#    #OpsRegistrator.cpp
#)

#
# DLA is built, by default, using the platform-provided set of:
# ScaLAPACK, BLACS, Open MPI, LAPACK, and BLAS
# (On most, but not all platforms, ScaLAPACK is a recent version that
#  contains BLACS internally, and is not a separate package)
#
# We will call this list of 4-5 packages "the ScaLAPACK stack"
#

set(USE_PLATFORM_PACKAGES "true") 

if(USE_ALTERNATE_STACK_LP64)
    unset(USE_PLATFORM_PACKAGES)
    message(STATUS "USE_ALTERNATE_STACK_LP64:  Scalapack configured for ALTERNATE_STACK_LP64")
    # The flags and paths for the ALTERNATE_STACK_LP64
    # would go here
    set(ALTROOT "/opt/intel/alt")
    set(ALT_INCD "${ALTROOT}/include")
    set(ALT_LIBD "${ALTROOT}/lib/intel64")
    # choose one of the following to select sequential or parallel:
    # set(ALT_PARALLEL 1) # if enabled, requires setting environement variables to limit num threads used
    if(ALT_PARALLEL)
        message(STATUS "ALT_PARALLEL:  Scalapack configured for parallel ALT BLAS")
        set(ALT_SEQ "gnu_thread") # threaded BLAS via openmp
        set(ALT_SEQ2 "-fopenmp;") #  openmp support, must end with ";"
    else()
        set(ALT_SEQ "sequential") # single-threaded BLAS OR
        set(ALT_SEQ2 "")          # nothing
    endif()

    set(SCALAPACK_LIBRARIES "${ALT_LIBD}/libalt_scalapack_lp64.a;-Wl,--start-group;${ALT_LIBD}/libalt_gf_lp64.a;${ALT_LIBD}/libalt_${ALT_SEQ}.a;${ALT_LIBD}/libalt_core.a;${ALT_LIBD}/libalt_blacs_openmpi_lp64.a;-Wl,--end-group;${ALT_SEQ2}-lpthread;-lm")
    set(LAPACK_LIBRARIES "")
    set(BLAS_LIBRARIES "")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -m64 -I${ALT_INC_DIR}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64 -I{ALT_INC_DIR}")  # not sure if this one is needed

    set(SCALAPACK_FOUND "1")        # emulate that finds worked
    set(LAPACK_FOUND "1")
    set(BLAS_FOUND "1")
endif(USE_ALTERNATE_STACK_LP64)

if(USE_ALT_ILP64)
    unset(USE_PLATFORM_PACKAGES)
    message(STATUS "USE_ALT_ILP64 set -- configuring for large-memory ALT Scalapack libs")
    #######################################
    # ALT ILP64 setup: (was working at one point, not tested recently)
    #
    # Later on, in the October timeframe, when building with ALT on RHEL 5. for the purpose of generating
    # binaries for commercial customers, we will want to use the following settings, which extend
    # ScaLAPACK to use 64-bit integers to describe the size of matrix WORK areas (as
    # well as every other INTEGER type in the ScaLAPACK FORTRAN code.
    #
    set(CMAKE_Fortran_FLAGS "${CMAKE_FORTRAN_FLAGS} -DALT_ILP64 -m64 -I${ALT_INC_DIR}")
    set(SCALAPACK_LIBRARIES "-L ${ALT_LIB_DIR} -lalt_scalapack_ilp64 -lalt_blacs_intelmpi_ilp64")
    set(LAPACK_LIBRARIES "-L ${ALT_LIB_DIR} -lalt_intel_ilp64 -lalt_sequential -lalt_core -lalt_rt -lalt_def -lpthread -lm")
    set(BLAS_LIBRARIES "-L ${ALT_LIB_DIR} -lalt_rt")
    # alt_avx or alt_def needed
endif(USE_ALT_ILP64)

if(USE_PLATFORM_PACKAGES)
    ######################
    # BLAS - from platform
    ######################
    set(USE_LOCAL_BLAS,0)
    find_package(BLAS)
    if(USE_LOCAL_BLAS)
        set(BLAS_LIBRARIES "-L/usr/local/lib")
        # if ATLAS BLAS, have to add:
        # set(BLAS_LIBRARIES, "$BLAS_LIBRARIES -lptcblas -lptf77blas -latlas ")
        # set(BLAS_LIBRARIES, "$BLAS_LIBRARIES -pthread /usr/lib/gcc/x86_64-linux-gnu/4.6.1/libgfortran.so")
    endif(USE_LOCAL_BLAS)

    if (BLAS_FOUND)
        #message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
    else(BLAS_FOUND)
        message(WARNING "BLAS not found. ScaLAPACK-based functionality will NOT be built")
	message(STATUS "Please look to doc/build.txt for list of dependencies")
    endif (BLAS_FOUND)

    ######################
    # LAPACK - from platform
    ######################
    set(USE_LOCAL_LAPACK 0)
    find_package(LAPACK)
    if(USE_LOCAL_LAPACK)
        set(LAPACK_LIBRARIES "-L/usr/local/lib -ltmg -lreflapack")  # DEBUG 
    endif(USE_LOCAL_LAPACK)

    if (LAPACK_FOUND)
        #message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
    else(LAPACK_FOUND)
        message(WARNING "LAPACK not found. ScaLAPACK-based functionality will NOT be built")
	message(STATUS "Please look to doc/build.txt for list of dependencies")
    endif(LAPACK_FOUND)

    ###########################
    # ScaLAPACK - from PLATFORM
    ###########################
    set(USE_LOCAL_SCALAPACK 0)
    if(USE_LOCAL_SCALAPACK)
        message(STATUS "WARNING-- using locally installed ScaLAPACK stack")
        set(SCALAPACK_LIBRARIES "/usr/local/lib/libscalapack.a")
        set(SCALAPACK_INC_DIR "/usr/local/include/BLACS/SRC")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I ${SCALAPACK_INC_DIR}")
    else()
        message(STATUS "Using platform ScaLAPACK stack")
	find_package(ScaLAPACK)
    endif()

    if (SCALAPACK_FOUND)
        #message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
    else (SCALAPACK_FOUND)
        message(WARNING "ScaLAPACK not found. ScaLAPACK-based functionality will NOT be built.")
	message(STATUS "Please look to doc/build.txt for list of dependencies")
    endif (SCALAPACK_FOUND)
endif(USE_PLATFORM_PACKAGES)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOMPI_IGNORE_CXX_SEEK")

if(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    message(WARNING "MPI not present. ScaLAPACK-based functionality will NOT be built")
    message(WARNING "Debug: MPI_C_FOUND is ${MPI_C_FOUND}, MPI_CXX_FOUND is ${MPI_CXX_FOUND}, MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "Ubuntu Hint: $ sudo apt-get install libopenmpi-dev")
endif(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MPI_INCLUDE}")

message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
message(STATUS "Debug: MPI_LIBRARIES is ${MPI_LIBRARIES}")
message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_FLAGS is ${CMAKE_Fortran_FLAGS}")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)
    message(STATUS "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")

    # we are not using OpenMPI tools (e.g. mpic++) to build because
    # only the slave needs to be built that way
    
    #
    # build the common mpi framework code
    #
    # TODO: this will return
#    set (dense_linear_algebra_common_src mpi/MPIUtils.cpp
#                                         sharedMem/SharedMemoryIpc.cpp
#    )

#   add_library(dense_linear_algebra_common STATIC ${dense_linear_algebra_common_src})
#   set_target_properties(dense_linear_algebra_common PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

    #
    # add the mpi test operator and scalapack-based DLA operators
    # to the dense_linear_algebra library
    #
    set (dense_linear_algebra_src ${dense_linear_algebra_src}
        plugin.cpp
        # scalapack emulation:
        dlaScaLA/scalapackEmulation/numroc.f       # called from Physical operators
        dlaScaLA/scalapackEmulation/descinit.f     # called from Physical operators
                dlaScaLA/scalapackEmulation/lsame.f     # compare two single letters!
            dlaScaLA/scalapackEmulation/pdelset.f       # pdelsetOp - phys op
            dlaScaLA/scalapackEmulation/pdelget.f       # pdelgetOp - phys op
                dlaScaLA/scalapackEmulation/infog2l.f   # local indices; referenced by elset/elget?
            dlaScaLA/scalapackEmulation/pxerbla.f  # error handler
        # BLACS emulation:
            dlaScaLA/scalapackEmulation/blacs_info_fake.c # blacs_grid_info() from Physical operators
        scalapackUtil/reformat.cpp
        scalapackUtil/test/MPICopyLogical.cpp
        scalapackUtil/test/MPICopyPhysical.cpp
        scalapackUtil/test/slaving/mpiCopyMaster.cpp
        scalapackUtil/test/MPIRankLogical.cpp
        scalapackUtil/test/MPIRankPhysical.cpp
        scalapackUtil/test/slaving/mpiRankMaster.cpp
        scalapackUtil/ScaLAPACKLogical.cpp
        scalapackUtil/ScaLAPACKPhysical.cpp
        dlaScaLA/GEMMLogical.cpp
        dlaScaLA/GEMMPhysical.cpp
        dlaScaLA/SVDLogical.cpp
        dlaScaLA/SVDPhysical.cpp
        dlaScaLA/slaving/pdgemmMaster.cpp
        dlaScaLA/slaving/pdgesvdMaster.cpp
    )

    #
    # build the mpi-slave program
    #
    set (mpi_slave_src
        scalapackUtil/test/slaving/mpiCopySlave.cpp
        scalapackUtil/test/slaving/mpiRankSlave.cpp
        dlaScaLA/slaving/pdgemmSlave.cpp
        dlaScaLA/slaving/pdgesvdSlave.cpp
        dlaScaLA/slaving/slaveTools.cpp
        scalapackUtil/reformat.cpp
    )

    # Temporarily, src/mpi/slaving produces scidb_mpi_slave, which is the "everything" slave
#    add_executable(mpi_slave ${mpi_slave_src})
#    if(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]")
#     target_link_libraries(mpi_slave ${LIBGFORTRAN_LIBRARIES})
#   endif(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]")
#   target_link_libraries(mpi_slave ${MPI_LIBRARIES})
#   target_link_libraries(mpi_slave ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${SCALAPACK_LIBRARIES})
#   target_link_libraries(mpi_slave ${LOG4CXX_LIBRARIES} ${PROTOBUF_LIBRARY} ${LIBRT_LIBRARIES} ${Boost_LIBRARIES})
#   TODO: restore this directory to  have its own mpi-slave

#    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "mpi_slave" mpi_slave)
#    set_target_properties(mpi_slave PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

#
# and the tools needed to test gemm and gesvd
#
configure_file(doSvd.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvd.sh" COPYONLY)
configure_file(doSvdWithMetrics.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdWithMetrics.sh" COPYONLY)
configure_file(doSvdMetric.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdMetric.sh" COPYONLY)
configure_file(diag.sh "${GENERAL_OUTPUT_DIRECTORY}/diag.sh" COPYONLY)
configure_file(test/driver.py "${GENERAL_OUTPUT_DIRECTORY}/dla_driver.py" COPYONLY)

else (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)

     set (dense_linear_algebra_src ${dense_linear_algebra_src}
          pluginNoSP.cpp
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

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

#
# complete the linar_algebra library settings
#
add_library(dense_linear_algebra SHARED ${dense_linear_algebra_src})

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)
    # target_link_libraries(dense_linear_algebra dense_linear_algebra_common)
    target_link_libraries(dense_linear_algebra mpi)
endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)
       
#message(STATUS "Debug: FINAL dense_linear_algebra_LIBRARIES =========================")
#message(STATUS "Debug: dense_linear_algebra_LIBRARIES is ${dense_linear_algebra_LIBRARIES}")
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libdense_linear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}" dense_linear_algebra)
set_target_properties(dense_linear_algebra PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

if (APPLE)
    set_target_properties(dense_linear_algebra PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()

