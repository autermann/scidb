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


if(BUILD_USER_DOC)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/xml/custom.xsl.in ${CMAKE_CURRENT_BINARY_DIR}/xml/custom.xsl)
    file(GLOB USER_DOC_SRC
        "xml/*.xml"
	"p4examples/*")
    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf
        COMMAND ${XSLTPROC_EXECUTABLE} --xinclude -o ${CMAKE_CURRENT_BINARY_DIR}/scidb-userguide-${SCIDB_SHORT_VERSION}.fo ${CMAKE_CURRENT_BINARY_DIR}/xml/custom.xsl ${CMAKE_CURRENT_SOURCE_DIR}/xml/scidb_ug.xml
        COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/pdf
        COMMAND ${FOP_EXECUTABLE} -fo ${CMAKE_CURRENT_BINARY_DIR}/scidb-userguide-${SCIDB_SHORT_VERSION}.fo -pdf ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf
        DEPENDS ${USER_DOC_SRC}
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/xml/
    )
    add_custom_target(user_doc ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf)
endif()
