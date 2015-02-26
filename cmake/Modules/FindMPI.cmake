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

# Wrapper around broken on RedHat FindMPI.cmake
set(CMAKE_SYSTEM_PREFIX_PATH_BACKUP ${CMAKE_SYSTEM_PREFIX_PATH})

if(${DISTRO_NAME_VER} MATCHES "RedHat-5")
  # RedHat 5.x
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi/1.4-gcc")
endif()

if(${DISTRO_NAME_VER} MATCHES "RedHat-6")
  # RedHat 6.x
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi")
endif()

if(${DISTRO_NAME_VER} MATCHES "Fedora")
  # Fedora 11/16/17
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi")
endif()

include(${CMAKE_ROOT}/Modules/FindMPI.cmake)

set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH_BACKUP})
