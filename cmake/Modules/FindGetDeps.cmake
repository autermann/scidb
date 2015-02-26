macro(GETDEPS Package Output Result)
    execute_process(COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/utils/depgen.py -f ${CMAKE_SOURCE_DIR}/dependencies.json -d ${DISTRO_NAME} -v ${DISTRO_VER} -p ${Package} -V ${SCIDB_SHORT_VERSION}
        OUTPUT_VARIABLE out
        RESULT_VARIABLE res
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    
    set(${Output} ${out})
    set(${Result} ${res})
endmacro()
