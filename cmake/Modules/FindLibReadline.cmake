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

#
# Try to find libreadline
#
# Once done this will define
#  LIBREADLINE_FOUND        - TRUE if libreadline found
#  LIBREADLINE_INCLUDE_DIRS - Where to find libreadline include sub-directory
#  LIBREADLINE_LIBRARIES    - List of libraries when using libreadline
#

# check cache
IF(LIBREADLINE_INCLUDE_DIRS)
  SET(LIBREADLINE_FIND_QUIETLY TRUE)
ENDIF(LIBREADLINE_INCLUDE_DIRS)

# find includes and libraries
FIND_PATH(LIBREADLINE_INCLUDE_DIRS readline/readline.h)
IF(LIBREADLINE_STATIC)
   message(STATUS "LIBREADLINE_STATIC is ON")
   SET(PREV_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
   SET(CMAKE_FIND_LIBRARY_SUFFIXES .a .so)
   FIND_LIBRARY(LIBREADLINE_LIBRARY readline)
   SET(CMAKE_FIND_LIBRARY_SUFFIXES ${PREV_CMAKE_FIND_LIBRARY_SUFFIXES})
ELSE(LIBREADLINE_STATIC)
   FIND_LIBRARY(LIBREADLINE_LIBRARY readline)
ENDIF(LIBREADLINE_STATIC)

# standard handling
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibReadline DEFAULT_MSG LIBREADLINE_LIBRARY LIBREADLINE_INCLUDE_DIRS)

IF(LIBREADLINE_FOUND)
  SET(LIBREADLINE_LIBRARIES ${LIBREADLINE_LIBRARY})
ELSE(LIBREADLINE_FOUND)
  SET(LIBREADLINE_LIBRARIES)
ENDIF(LIBREADLINE_FOUND)
