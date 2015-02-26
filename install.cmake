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

find_package(GetDeps)

# If disto is unknown we will not create package at all
if(NOT ${DISTRO_NAME_VER} STREQUAL "")
    # CPack configuration
    set(CPACK_PACKAGE_NAME "scidb")
    set(CPACK_PACKAGE_VERSION ${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}.${SCIDB_VERSION_PATCH}.${SCIDB_REVISION}-${SCIDB_VERSION_CODENAME})
    set(CPACK_PACKAGE_VERSION_MAJOR ${SCIDB_VERSION_MAJOR})
    set(CPACK_PACKAGE_VERSION_MINOR ${SCIDB_VERSION_MINOR})
    set(CPACK_PACKAGE_VERSION_PATCH ${SCIDB_VERSION_PATCH})
    set(CPACK_PACKAGE_CONTACT "support@lists.scidb.org")
    set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "SciDB - database for extra large data processing")
    set(CPACK_PACKAGE_DESCRIPTION "")

    if(UNIX)
        set(CPACK_SET_DESTDIR "ON")
        set(CPACK_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")
    endif()

    #We not use default 'make package' target so just clean dependencies
    set(PACKAGE_DEPS "")

    # D E B   P A C K A G E
    if ("${CPACK_GENERATOR}" STREQUAL "DEB")
        execute_process(COMMAND dpkg --print-architecture
            OUTPUT_VARIABLE  CPACK_DEBIAN_PACKAGE_ARCHITECTURE  OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(CPACK_PACKAGE_FILE_NAME 
            "${CPACK_PACKAGE_NAME}-${CMAKE_BUILD_TYPE}-${CPACK_PACKAGE_VERSION}-${DISTRO_NAME_VER}-${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}")
        set(CPACK_DEBIAN_PACKAGE_DEPENDS "${PACKAGE_DEPS}")
        set(CPACK_CONFIGURE_VERSION_STRING "@TMP_COMPONENT_NAME@ (>=@SCIDB_SHORT_VERSION@)")
    endif ("${CPACK_GENERATOR}" STREQUAL "DEB")

    # R P M   P A C K A G E
    if ("${CPACK_GENERATOR}" STREQUAL "RPM")
        SET(CPACK_PACKAGE_RELOCATABLE "false")
        set(CPACK_RPM_PACKAGE_DEBUG "false")
        set(CPACK_RPM_PACKAGE_AUTOREQ " no")
        set(CPACK_RPM_SPEC_INSTALL_POST "/bin/true")
        execute_process(COMMAND uname -m
            OUTPUT_VARIABLE  CPACK_RPM_PACKAGE_ARCHITECTURE  OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(CPACK_PACKAGE_FILE_NAME 
            "${CPACK_PACKAGE_NAME}-${CMAKE_BUILD_TYPE}-${CPACK_PACKAGE_VERSION}-${DISTRO_NAME_VER}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")
        set(CPACK_RPM_PACKAGE_REQUIRES "${PACKAGE_DEPS}")
        set(CPACK_CONFIGURE_VERSION_STRING "@TMP_COMPONENT_NAME@ >= @SCIDB_SHORT_VERSION@")
    endif ("${CPACK_GENERATOR}" STREQUAL "RPM")

    # C O M P O N E N T S
    #libscidbclient pakcage
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/libscidbclient${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib COMPONENT libscidbclient)
    set(CPACK_COMPONENTS_ALL libscidbclient)
    set(libscidbclient_SUMMARY "SciDB client library")

    #scidb-utils package
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/iquery" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/gen_matrix" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/benchGen" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/csv2scidb" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/ssdbgen" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidbLoadCsv.sh" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/splitcsv" DESTINATION bin COMPONENT scidb-utils)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/loadcsv.py" DESTINATION bin COMPONENT scidb-utils)
    list(APPEND CPACK_COMPONENTS_ALL scidb-utils)
    set(scidb-utils_SUMMARY "SciDB querying tool and other utilities")
 
    #scidb-dev-tools package
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidbtestharness" DESTINATION bin COMPONENT scidb-dev-tools)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/arg_separator" DESTINATION bin COMPONENT scidb-dev-tools)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidbtestprep.py" DESTINATION bin COMPONENT scidb-dev-tools)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/mu_admin.py" DESTINATION bin COMPONENT scidb-dev-tools)
    install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/daemon.py" DESTINATION etc COMPONENT scidb-dev-tools)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/mu_config.ini" DESTINATION etc COMPONENT scidb-dev-tools)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/log4j.properties" DESTINATION etc COMPONENT scidb-dev-tools)
    list(APPEND CPACK_COMPONENTS_ALL scidb-dev-tools)
    set(scidb-dev-tools_SUMMARY "SciDB developer and tester utilities")

    #scidb-plugins package
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libpoint${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libmatch${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libbestmatch${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/librational${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libcomplex${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libra_decl${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libmore_math${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libmisc${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libfindstars${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libgroupstars${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libfits${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libmpi${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/libdense_linear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    if (TARGET scidb_mpi_slave)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/plugins/scidb_mpi_slave" DESTINATION lib/scidb/plugins COMPONENT scidb-plugins)
    endif ()
    list(APPEND CPACK_COMPONENTS_ALL scidb-plugins)
    set(scidb-plugins_SUMMARY "SciDB server and client plugins")

    #scidb package
    if (NOT WITHOUT_SERVER)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidb" DESTINATION bin COMPONENT scidb)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidbconf" DESTINATION bin COMPONENT scidb)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidb-prepare-db.sh" DESTINATION bin COMPONENT scidb)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/init-db.sh" DESTINATION bin COMPONENT scidb)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/scidb.py" DESTINATION bin COMPONENT scidb)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb_cores" DESTINATION bin COMPONENT scidb)

        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/data/meta.sql" DESTINATION share/scidb COMPONENT scidb)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/log4cxx.properties" DESTINATION share/scidb COMPONENT scidb)

        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb-sample.conf" DESTINATION etc COMPONENT scidb)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/sample_config.ini" DESTINATION etc COMPONENT scidb)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/config.ini.planet" DESTINATION etc COMPONENT scidb)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/config.ini.ec2" DESTINATION etc COMPONENT scidb)

        #set_target_properties(scidb
        #    PROPERTIES POST_INSTALL_SCRIPT ${CMAKE_SOURCE_DIR}/cmake/post_install.cmake
        #)

        list(APPEND CPACK_COMPONENTS_ALL scidb)
        set(scidb_SUMMARY "SciDB - database for extra large data processing")
    endif()

    if(SWIG_FOUND AND PYTHONLIBS_FOUND AND NOT APPLE)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/_libscidbpython${CMAKE_SHARED_LIBRARY_SUFFIX}" DESTINATION lib COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/libscidbpython.py" DESTINATION lib COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/scidbapi.py" DESTINATION lib COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/README" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/sample.py" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/simplearray.data" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/sample2.py" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/sample2.csv" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/pythonexamples/log4cxx.properties" DESTINATION share/scidb/examples/python COMPONENT libscidbclient-python)

        list(APPEND CPACK_COMPONENTS_ALL libscidbclient-python)
        set(libscidbclient-python_SUMMARY "SciDB client library python connector")
    endif(SWIG_FOUND AND PYTHONLIBS_FOUND AND NOT APPLE)

    #scidb-dev package
    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/ DESTINATION include COMPONENT scidb-dev PATTERN ".svn" EXCLUDE)
    list(APPEND CPACK_COMPONENTS_ALL scidb-dev)
    set(scidb-dev_SUMMARY "SciDB headers")

    if(SCIDB_DOC_TYPE STREQUAL "FULL" OR SCIDB_DOC_TYPE STREQUAL "API" AND BUILD_USER_DOC)
        #scidb-doc package
        file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/doc/api/html/) #create doxygen out dir so make install will work even without 'make doc'
        install(DIRECTORY ${CMAKE_BINARY_DIR}/doc/api/html/ DESTINATION share/doc/api COMPONENT scidb-doc PATTERN ".svn" EXCLUDE)
        install(DIRECTORY ${CMAKE_BINARY_DIR}/doc/user/pdf/ DESTINATION share/doc/user COMPONENT scidb-doc PATTERN ".svn" EXCLUDE)
        list(APPEND CPACK_COMPONENTS_ALL scidb-doc)
        set(scidb-doc_SUMMARY "SciDB documentation")
    endif()

    # D E B U G   P A C K A G E S
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo" AND NOT APPLE)
        #libscidbclient-dbg package
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/libscidbclient${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT libscidbclient-dbg)
        list(APPEND CPACK_COMPONENTS_ALL libscidbclient-dbg)
        set(libscidbclient-dbg_SUMMARY "${libscidbclient_SUMMARY} (debug symbols)")

        #scidb-utils-dbg package
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/iquery${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/gen_matrix${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/benchGen${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/csv2scidb${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/ssdbgen${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/splitcsv${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-utils-dbg)
        list(APPEND CPACK_COMPONENTS_ALL scidb-utils-dbg)
        set(scidb-utils-dbg_SUMMARY "${scidb-utils_SUMMARY} (debug symbols)")

        #scidb-dev-tools-dbg package
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/scidbtestharness${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-dev-tools-dbg)
        install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/arg_separator${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-dev-tools-dbg)
        list(APPEND CPACK_COMPONENTS_ALL scidb-dev-tools-dbg)
        set(scidb-dev-tools-dbg_SUMMARY "${scidb-dev-tools_SUMMARY} (debug symbols)")

        #scidb-dbg package
        if (NOT WITHOUT_SERVER)
            install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/scidb${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-dbg)
            install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/scidbconf${DEBUG_SYMBOLS_EXTENSION}" DESTINATION bin/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-dbg)
            list(APPEND CPACK_COMPONENTS_ALL scidb-dbg)
            set(scidb-dbg_SUMMARY "${scidb_SUMMARY} (debug symbols)")
        endif()

        #scidb-plugins-dbg package
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libpoint${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libmatch${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libbestmatch${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/librational${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libcomplex${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libra_decl${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libmore_math${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libmisc${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libfits${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libgroupstars${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libfindstars${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libmpi${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        install(FILES "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/libdense_linear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT scidb-plugins-dbg)
        if (TARGET scidb_mpi_slave)
            install(PROGRAMS "${GENERAL_OUTPUT_DIRECTORY}/plugins/${DEBUG_SYMBOLS_DIRECTORY}/scidb_mpi_slave${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/scidb/plugins/${DEBUG_SYMBOLS_EXTENSION} COMPONENT scidb-plugins-dbg)
        endif ()

        list(APPEND CPACK_COMPONENTS_ALL scidb-plugins-dbg)
        set(scidb-plugins-dbg_SUMMARY "${scidb-plugins_SUMMARY} (debug symbols)")

        #libscidbclient-python-dbg package
        if(SWIG_FOUND AND PYTHONLIBS_FOUND AND NOT APPLE)
            install(FILES "${GENERAL_OUTPUT_DIRECTORY}/${DEBUG_SYMBOLS_DIRECTORY}/_libscidbpython${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}" DESTINATION lib/${DEBUG_SYMBOLS_DIRECTORY} COMPONENT libscidbclient-python-dbg)
            list(APPEND CPACK_COMPONENTS_ALL libscidbclient-python-dbg)
            set(libscidbclient-python-dbg_SUMMARY "${libscidbclient-python_SUMMARY} (debug symbols)")
        endif()
    endif ()
    # E N D   C O M P O N E N T S
	
    if(NOT APPLE)
        #generate custom CPack rules for each component
        set(PACKAGING_CMDS)
        foreach(component ${CPACK_COMPONENTS_ALL})
            getdeps(${component} COMPONENT_PACKAGE_DEPS RES)
            if (NOT ${RES} EQUAL 0)
                message(WARNING "Can not generate dependencies list for package ${component}: ${COMPONENT_PACKAGE_DEPS}")
                set(COMPONENT_PACKAGE_DEPS "")
            endif()

            message(STATUS "Dependencies which will be used during packaging component ${component}: ${COMPONENT_PACKAGE_DEPS}")

            set(CONFIGURE_CMAKE_PROJECT_NAME ${CMAKE_PROJECT_NAME})
            set(CONFIGURE_COMPONENT ${component})
            set(CONFIGURE_CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE})
            set(CONFIGURE_DISTRO_NAME_VER ${DISTRO_NAME_VER})
            set(CONFIGURE_COMPONENT_DEPS "${COMPONENT_PACKAGE_DEPS}")
            set(CONFIGURE_PACKAGE_SUMMARY ${${component}_SUMMARY})

            configure_file(
                "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CPackConfig.cmake.in"
                "${CMAKE_CURRENT_BINARY_DIR}/CPackConfig-${component}.cmake"
                IMMEDIATE @ONLY)

            set(PACKAGING_CMD COMMAND cpack --config ${CMAKE_CURRENT_BINARY_DIR}/CPackConfig-${component}.cmake)
            add_custom_target(${component}-package ${PACKAGING_CMD})
            list(APPEND PACKAGING_CMDS ${PACKAGING_CMD})
        endforeach()

        add_custom_target(packages ${PACKAGING_CMDS})

        # Very ugly solution of #1657. I have no better idea. CMAKE must support system tar instead of BSD style for DEBIAN. I hope the problem of cmake will be resolved soon.
        # Originally this solution was proposed in this email http://permalink.gmane.org/gmane.comp.programming.tools.cmake.user/42421 for like problem
        if ("${CPACK_GENERATOR}" STREQUAL "DEB" AND (SCIDB_DOC_TYPE STREQUAL "FULL" OR SCIDB_DOC_TYPE STREQUAL "API" AND BUILD_USER_DOC) )
            set(DOC_COMPONENT_FILENAME "${CMAKE_CURRENT_BINARY_DIR}/scidb-doc-${CMAKE_BUILD_TYPE}-${CPACK_PACKAGE_VERSION}-${DISTRO_NAME_VER}-${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}.deb")
            file(REMOVE_RECURSE "${CMAKE_CURRENT_BINARY_DIR}/tmp/")
            file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp/")
            add_custom_command(TARGET packages
                POST_BUILD
                COMMAND ar x "${DOC_COMPONENT_FILENAME}" data.tar.gz
                COMMAND tar xzf data.tar.gz
                COMMAND tar zcf data.tgz ./opt
                COMMAND mv data.tgz data.tar.gz
                COMMAND ar r "${DOC_COMPONENT_FILENAME}" data.tar.gz
                WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp/")
        endif()
    endif()

    include(CPack)
else()
    message(WARNING "Unknown distro or OS. Unable to create binary packages!")
endif()

# S O U R C E   P A C K A G E
set(CPACK_SRC_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")

add_custom_target(src_package
    COMMAND rm -rf ${CPACK_SRC_PACKAGE_FILE_NAME}
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/${CPACK_SRC_PACKAGE_FILE_NAME}.tgz
    COMMAND svn export ${CMAKE_SOURCE_DIR} ${CPACK_SRC_PACKAGE_FILE_NAME}
    COMMAND cp ${CMAKE_BINARY_DIR}/version.txt ${CPACK_SRC_PACKAGE_FILE_NAME}
    COMMAND tar -czf ${CMAKE_BINARY_DIR}/${CPACK_SRC_PACKAGE_FILE_NAME}.tgz ${CPACK_SRC_PACKAGE_FILE_NAME}
    WORKING_DIRECTORY /tmp
    )

