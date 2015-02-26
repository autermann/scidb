find_package(Java6)

if (NOT JAVA6_FOUND)
   message(FATAL_ERROR "Java 1.6 is currently the only supported version for builing JDBC!")
endif()

set (ANT_BUILD_FILE ${CMAKE_CURRENT_SOURCE_DIR}/build.xml)
set (BUILD_DIR  ${CMAKE_CURRENT_BINARY_DIR}/build)
set (DIST_DIR   ${GENERAL_OUTPUT_DIRECTORY}/jdbc)
set (DEPS_DIR   ${CMAKE_CURRENT_BINARY_DIR}/deps)
set (REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/report)
set (BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set (PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../../src/network/proto/)

message(STATUS "PROTO_DIR = ${PROTO_DIR}")

execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/src
                                                           ${CMAKE_CURRENT_BINARY_DIR}/src)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/lib
                                                           ${CMAKE_CURRENT_BINARY_DIR}/lib)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/logging.properties
                                                           ${CMAKE_CURRENT_BINARY_DIR}/logging.properties)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j.spec
                                                           ${CMAKE_CURRENT_BINARY_DIR}/scidb4j.spec)
set (ENV{JAVA_HOME} "${Java6_HOME}")

add_custom_target ( jdbc_build ALL )

add_custom_command ( TARGET jdbc_build
		   COMMAND export JAVA_HOME=${Java6_HOME}\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR} configure
		   COMMAND export JAVA_HOME=${Java6_HOME}\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR}
		   )
#-----------------------------------------------------------------------------
# To make the rpm, ant has to be called with the target name rpm (e.g. ant -f ${ANT_BUILD_FILE} rpm)
#-----------------------------------------------------------------------------

	 
