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

cmake_minimum_required(VERSION 2.6)

project(SciDB C CXX Fortran)

set(SCIDB_VERSION_MAJOR "12")
set(SCIDB_VERSION_MINOR "10")
set(SCIDB_VERSION_PATCH "0")
set(SCIDB_VERSION_CODENAME "Cheshire-final")

set(SCIDB_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}.${SCIDB_VERSION_PATCH}")
set(SCIDB_SHORT_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}")

set(PACKAGE_NAME "scidb-${SCIDB_SHORT_VERSION}")

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/scidb/${SCIDB_SHORT_VERSION}" CACHE STRING "Install path" FORCE)
  message(STATUS "CMAKE_INSTALL_PREFIX Initialized to default setting to: " ${CMAKE_INSTALL_PREFIX})
else(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  message(STATUS "CMAKE_INSTALL_PREFIX not Initialized to default keeping: " ${CMAKE_INSTALL_PREFIX})
endif(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

find_package(Subversion)
if (${CMAKE_VERSION} VERSION_GREATER 2.8 OR ${CMAKE_VERSION} VERSION_EQUAL 2.8)
    find_package(Git)
endif()
if(Subversion_SVN_EXECUTABLE)
    EXECUTE_PROCESS(COMMAND ${Subversion_SVN_EXECUTABLE} info . OUTPUT_VARIABLE Subversion_INFO_OUT
        ERROR_VARIABLE Subversion_svn_info_error RESULT_VARIABLE Subversion_svn_info_result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (Subversion_svn_info_result EQUAL 0)
        # Get version from SVN working copy by default
        Subversion_WC_INFO(${CMAKE_SOURCE_DIR} SciDB)
        set(SCIDB_REVISION "${SciDB_WC_REVISION}")
        message(STATUS "Reading revision from svn: ${SCIDB_REVISION}")
        file(WRITE "${CMAKE_BINARY_DIR}/version.txt" ${SCIDB_REVISION})
    endif()
endif()
if(NOT SCIDB_REVISION AND GIT_EXECUTABLE AND EXISTS "${CMAKE_SOURCE_DIR}/.git")
    # Get version from git-svn working copy 
    execute_process(COMMAND ${GIT_EXECUTABLE} svn find-rev master
        OUTPUT_VARIABLE SCIDB_REVISION
        RESULT_VARIABLE GIT_EXEC_RES
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    string(COMPARE NOTEQUAL ${GIT_EXEC_RES} 0 GIT_EXEC_RES) 
    if(${GIT_EXEC_RES})
        MESSAGE(FATAL_ERROR "Can not invoke GIT ${GIT_EXECUTABLE}. Check binary!")
    endif()
    
    message(STATUS "Reading revision from git-svn: ${SCIDB_REVISION}")

    file(WRITE "${CMAKE_BINARY_DIR}/version.txt" ${SCIDB_REVISION})
endif()
if(NOT SCIDB_REVISION AND EXISTS "${CMAKE_SOURCE_DIR}/version.txt")
    # Get version from plain source tarball/directory    
    file(READ "${CMAKE_SOURCE_DIR}/version.txt" SCIDB_REVISION)
    message(STATUS "Reading revision from file: ${SCIDB_REVISION}")
endif()
if(NOT SCIDB_REVISION)
    message(FATAL_ERROR "Can not fetch working copy version and can't find version.txt file.")
endif()

set(SCIDB_VERSION "${SCIDB_VERSION}.${SCIDB_REVISION} (${SCIDB_VERSION_CODENAME})")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

message(STATUS "Build type (use -DCMAKE_BUILD_TYPE=[RelWithDebInfo]/Debug/Release/Profile/CC/Valgrind): ${CMAKE_BUILD_TYPE}")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo) # Default value for end user who want to install scidb from sources
endif(NOT CMAKE_BUILD_TYPE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic -W -Wextra -Wall -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -fPIC -D__STDC_FORMAT_MACROS")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-system-headers -isystem /opt/local/include/ -isystem /usr/local/include/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__STDC_LIMIT_MACROS")
if ("${CMAKE_GENERATOR}" STREQUAL "Xcode")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPROJECT_ROOT=\\\\\"${CMAKE_SOURCE_DIR}/\\\\\"")
else ("${CMAKE_GENERATOR}" STREQUAL "Xcode")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPROJECT_ROOT=\\\"${CMAKE_SOURCE_DIR}/\\\"")
endif ("${CMAKE_GENERATOR}" STREQUAL "Xcode")

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -DNDEBUG -ggdb3")
set(CMAKE_CXX_FLAGS_PROFILE "-O2 -pg -DNDEBUG")
set(CMAKE_CXX_FLAGS_CC "-fprofile-arcs -ftest-coverage -g -O0 -DCLEAN_EXIT")
set(CMAKE_CXX_FLAGS_VALGRIND "-g -O0 -DCLEAN_EXIT")

set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -DNDEBUG -ggdb3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pedantic -W -Wextra -Wall -Wno-long-long -Wno-unused-parameter -fPIC -D__STDC_FORMAT_MACROS")
set(CMAKE_C_FLAGS_PROFILE "-O2 -pg -DNDEBUG")
set(CMAKE_C_FLAGS_CC "-fprofile-arcs -ftest-coverage -g -O0 -DCLEAN_EXIT")
set(CMAKE_C_FLAGS_VALGRIND "-g -O0 -DCLEAN_EXIT")

set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -fno-f2c")
set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -I/usr/include/mpi")
set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} ${ALL_FLAGS}")
set(CMAKE_Fortran_FLAGS_RELWITHDEBINFO "${CMAKE_Fortran_RELWITHDEBINFO} -DNDEBUG -ggdb3")
set(CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_DEBUG} -DDEBUG")  # Just for detection debug mode in code
set(CMAKE_Fortran_FLAGS_PROFILE "${CMAKE_Fortran_PROFILE} -O2 -pg -DNDEBUG")
set(CMAKE_Fortran_FLAGS_CC "${CMAKE_Fortran_FLAGS_CC} -fprofile-arcs -ftest-coverage")

set(CMAKE_EXE_LINKER_FLAGS_PROFILE "-pg")
set(CMAKE_LDFLAGS_CC "-fprofile-arcs -ftest-coverage -g -DCLEAN_EXIT")

set(GENERAL_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(GENERAL_TEST_DIRECTORY "${CMAKE_BINARY_DIR}/tests/basic")
set(GENERAL_HARNESSTEST_DIRECTORY "${CMAKE_BINARY_DIR}/tests/harness")

set(PYTHON_SAMPLE_DIRECTORY "${CMAKE_BINARY_DIR}/bin/pythonexamples")

# We not overriding RPATH for all binaries. Just setting some handy paths.
set(DEFAULT_RPATH "$ORIGIN") # This for working from developer catalog
list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
if("${isSystemDir}" STREQUAL "-1")
	#This one for installed binaris. Note if system prefix used RPATH not setted.
	set (DEFAULT_RPATH "${DEFAULT_RPATH}:${CMAKE_INSTALL_PREFIX}/lib")
endif("${isSystemDir}" STREQUAL "-1")

include(FindPkgConfig)
include(LinuxDistroVersion)

# Set option for static linking
message("DISTRO_NAME_VER is " ${DISTRO_NAME_VER})
set(LIBREADLINE_STATIC OFF)

if(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]" OR ${DISTRO_NAME_VER} MATCHES "SUSE")
    set(Boost_USE_STATIC_LIBS ON)
    set(LOG4CXX_USE_STATIC_LIBS ON)
    set(PROTOBUF_USE_STATIC_LIBS ON)
    set(LIBREADLINE_STATIC ON)
    find_package(Curses REQUIRED)
    message(STATUS "This is Red Hat based distro and we switch on static building for some libs")
endif()

if(${DISTRO_NAME_VER} MATCHES "Fedora") 
    string(REPLACE "Fedora-" "" FEDORA_VERSION "${DISTRO_NAME_VER}")
    if (FEDORA_VERSION LESS "16")
      set(Boost_USE_STATIC_LIBS ON)
      message(STATUS "This is Fedora less then 16 and we switch on static building for boost")
    endif(FEDORA_VERSION LESS "16")
endif(${DISTRO_NAME_VER} MATCHES "Fedora")

find_package(Boost REQUIRED system program_options serialization regex filesystem thread)
find_package(Protobuf REQUIRED)
find_package(Log4CXX REQUIRED)
find_package(Doxygen)
find_package(LibPQXX REQUIRED)
find_package(LibPQ REQUIRED)

if (APPLE)
    FIND_PATH(CPPUNIT_INCLUDE_DIR cppunit/TestCase.h /usr/local/include /usr/include)
    FIND_LIBRARY(CPPUNIT_LIBRARY cppunit ${CPPUNIT_INCLUDE_DIR}/../lib /usr/local/lib /usr/lib)
    FIND_LIBRARY(CPPUNIT_DEBUG_LIBRARY cppunit ${CPPUNIT_INCLUDE_DIR}/../lib /usr/local/lib /usr/lib)
    IF(CPPUNIT_INCLUDE_DIR)
        IF(CPPUNIT_LIBRARY)
            SET(CPPUNIT_FOUND "YES")
            SET(CPPUNIT_LIBRARIES ${CPPUNIT_LIBRARY} ${CMAKE_DL_LIBS})
            SET(CPPUNIT_DEBUG_LIBRARIES ${CPPUNIT_DEBUG_LIBRARY} ${CMAKE_DL_LIBS})
        ENDIF()
    ENDIF()

    if (NOT WITHOUT_PYTHON_BINDING)
        message(WARNING "Python binding broken on Mac! Forcing WITHOUT_PYTHON_BINDING=1")
    endif()
    set(WITHOUT_PYTHON_BINDING 1)
else ()
    pkg_check_modules(CPPUNIT cppunit)

    find_package(LibRT REQUIRED)

    if (NOT WITHOUT_PYTHON_BINDING)
        find_package(SWIG "2.0")
        find_package(PythonLibs)
        if (NOT SWIG_FOUND OR NOT PYTHONLIBS_FOUND)
	        message(WARNING "SWIG 2 and Python headers required for building Python API bindings. You can set proper SWIG executable with argument -DSWIG_EXECUTABLE=/path/to/swig2.0")
            set(WITHOUT_PYTHON_BINDING 1)
        endif()
    endif()
endif()

set(FLEX_MINIMAL_VERSION "2.5.35")
find_package(FLEX REQUIRED)
set(BISON_MINIMAL_VERSION "2.4")
find_package(BISON REQUIRED)
find_package(SED REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LibReadline REQUIRED)
find_package(Threads REQUIRED)
find_package(XsltProc)
find_package(Fop)
find_package(DocBook)
find_package(PythonInterp REQUIRED)
if(XSLTPROC_EXECUTABLE AND FOP_EXECUTABLE AND DOCBOOK_XSL_FILE)
    set(BUILD_USER_DOC 1)
else()
    set(BUILD_USER_DOC 0)
endif()

# System specific constants. Result will be written into src/system/System.h
include(CheckFunctionExists)
check_function_exists(malloc_stats HAVE_MALLOC_STATS)

include_directories("${CMAKE_SOURCE_DIR}/src")
include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("${CMAKE_SOURCE_DIR}/utils")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/src")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/utils")
include_directories("${CMAKE_SOURCE_DIR}/extern/bsdiff")
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PROTOBUF_INCLUDE_DIR})
include_directories(${LOG4CXX_INCLUDE_DIRS})
include_directories(${LIBPQXX_INCLUDE_DIRS})
include_directories(${LIBPQ_INCLUDE_DIRS})
include_directories(${ZLIB_INCLUDE_DIRS})
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${READLINE_INCLUDE_DIRS})
include_directories("${CMAKE_SOURCE_DIR}/extern")

link_directories(${LIBPQXX_LIBRARY_DIRS})
link_directories(${LIBPQ_LIBRARY_DIRS})

# static link requires adding a few more libraries
set (LINK_APR_LIBS "")

# macroses
set(DEBUG_SYMBOLS_DIRECTORY ".debug")
set(DEBUG_SYMBOLS_EXTENSION "${DEBUG_SYMBOLS_DIRECTORY}")

macro(extractDebugInfo input_dir input_file targ)
    if("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
		if (NOT APPLE)
	        set(DEBUG_SYMBOLS_OUTPUT_DIRECTORY "${input_dir}/${DEBUG_SYMBOLS_DIRECTORY}")

	        file(MAKE_DIRECTORY "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}")
	        add_custom_command(TARGET ${targ}
	            POST_BUILD
	            COMMAND objcopy --only-keep-debug "${input_dir}/${input_file}" "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION}"
	            COMMAND strip --strip-debug --strip-unneeded "${input_dir}/${input_file}"
	            COMMAND objcopy --add-gnu-debuglink=${DEBUG_SYMBOLS_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION} ${input_dir}/${input_file}
	            COMMAND chmod -x "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION}"
	            WORKING_DIRECTORY "${input_dir}"
	        )
		endif()
    endif()
endmacro(extractDebugInfo)

add_subdirectory("extern")
add_subdirectory("src")
add_subdirectory("utils")
add_subdirectory("doc")

if (NOT WITHOUT_SERVER)
    add_subdirectory("tests")
    add_subdirectory("examples")
endif()

## # The following are required to uses Dart and the Cdash dashboard
ENABLE_TESTING()
INCLUDE(CTest)

ADD_TEST(NightlyTest "/bin/bash" ${GENERAL_HARNESSTEST_DIRECTORY}/runharness.sh ${GENERAL_HARNESSTEST_DIRECTORY})
set_tests_properties (NightlyTest PROPERTIES TIMEOUT 16200)


#
# Custom uninstall target
#
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake)

# These are dummy target for adding misc files into the IDE projects like QtCreator
# for easy access to edit

# Add here your files or masks
file(GLOB MISC_FILES_SRC
    "doc/pages/*.dox"
    "doc/*.htm*"
    "src/capi/scidbpython.i"
    "doc/api/*"
    "doc/*.txt"
)

# Mirror the test and doc files to the build area

execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/tests
                                                           ${CMAKE_BINARY_DIR}/tests)

execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/doc
                                                           ${CMAKE_BINARY_DIR}/doc)

add_library(misc_files EXCLUDE_FROM_ALL examples/point/point.cpp ${MISC_FILES_SRC})
# End of dummy target

# Installation module include
include(install.cmake)
