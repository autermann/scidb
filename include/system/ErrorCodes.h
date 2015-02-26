/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2012 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

/**
 * @file ErrorCodes.h
 *
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Built-in error codes
 */

#ifndef ERRORCODES_H_
#define ERRORCODES_H_

#include <stdint.h>

#define SCIDB_MAX_SYSTEM_ERROR 0xFFFF
#define SCIDB_USER_ERROR_CODE_START (SCIDB_MAX_SYSTEM_ERROR+1)

namespace scidb
{

#define ERRCODE(name, code) const int32_t name = code

ERRCODE(SCIDB_E_NO_ERROR,                          0); //This one used for long and short messages because it't not really error

//Short error codes
ERRCODE(SCIDB_SE_QPROC,                             1); //Query processor error
ERRCODE(SCIDB_SE_STORAGE,                           2); //Storage error
ERRCODE(SCIDB_SE_SYSCAT,                            3); //System catalog error
ERRCODE(SCIDB_SE_ERRORS_MGR,                        4); //Errors manager error
ERRCODE(SCIDB_SE_UDO,                               5); //Error in user defined object
ERRCODE(SCIDB_SE_TYPE,                              6); //Type error
ERRCODE(SCIDB_SE_TYPESYSTEM,                        7); //Typesystem error
ERRCODE(SCIDB_SE_INTERNAL,                          8); //Internal SciDB error
ERRCODE(SCIDB_SE_PLUGIN_MGR,                        9); //Plugin manager error
ERRCODE(SCIDB_SE_OPERATOR,                          10); //Operator error
ERRCODE(SCIDB_SE_IO,                                11); //I/O error
ERRCODE(SCIDB_SE_METADATA,                          12); //Metadata error
ERRCODE(SCIDB_SE_NO_MEMORY,                         13); //Not enough memory
ERRCODE(SCIDB_SE_EXECUTION,                         14); //Error during query execution
ERRCODE(SCIDB_SE_CONFIG,                            15); //Error in config
ERRCODE(SCIDB_SE_NETWORK,                           16); //Network error
ERRCODE(SCIDB_SE_SYNTAX,                            17); //Query syntax error
ERRCODE(SCIDB_SE_OPTIMIZER,                         18); //Query optimizer error
ERRCODE(SCIDB_SE_THREAD,                            19); //Thread sync primitive error
ERRCODE(SCIDB_SE_PARSER,                            20); //Query parser error
ERRCODE(SCIDB_SE_INFER_SCHEMA,                      21); //Error during schema inferring
ERRCODE(SCIDB_SE_DBLOADER,                          22); //Error in loader
ERRCODE(SCIDB_SE_INJECTED_ERROR,                    23); //Injected error
ERRCODE(SCIDB_SE_IMPORT_ERROR,                      24); //Import error
ERRCODE(SCIDB_SE_MERGE,                             25); //Merge error
ERRCODE(SCIDB_SE_REDISTRIBUTE,                      26); //Error during redistribute
ERRCODE(SCIDB_SE_TYPE_CONVERSION,                   27); //Type conversion error
ERRCODE(SCIDB_SE_REPLICATION,                       28); // Replication error
//Next short ERRCODE

/*
 * Error manager error codes
 */
ERRCODE(SCIDB_LE_UNKNOWN_ERROR,                    -1); //Unknown error: %1%
ERRCODE(SCIDB_LE_ERRNS_ALREADY_REGISTERED,          1); //Errors namespace '%1%' already registered
ERRCODE(SCIDB_LE_ERRNS_CANT_BE_REGISTERED,          2); //Errors namespace '%1%' can not be unregistered
ERRCODE(SCIDB_LE_AGGREGATE_NOT_FOUND,               4); //Aggregate name '%1%' is not registered in the system
ERRCODE(SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK, 5); //Aggregate '%1%' does not support asterisk
ERRCODE(SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE,     6); //Aggregate '%1%' does not support input type '%2%'
ERRCODE(SCIDB_LE_UNHANDLED_VAR_PARAMETER,           7); //Operator '%1%' can not handle variable number of parameters
ERRCODE(SCIDB_LE_NO_MEMORY_FOR_VALUE,               8); //No memory for new Value
ERRCODE(SCIDB_LE_THREAD_EVENT_ERROR,                9); //Error state '%1%' detected in event wait
ERRCODE(SCIDB_LE_THREAD_SEMAPHORE_ERROR,            10); //Error state '%1%' detected in semaphore enter
ERRCODE(SCIDB_LE_CANT_LOAD_MODULE,                  11); //Can not load module '%1%', dlopen returned '%2%'
ERRCODE(SCIDB_LE_TOO_NEW_MODULE,                    12); //Too new plugin version. Version of %1% is %2%.%3%.%4%.%5% but but SciDB version is %6%
ERRCODE(SCIDB_LE_CANT_FIND_SYMBOL,                  13); //Can not find symbol '%1%', dlsym returned '%2%'
ERRCODE(SCIDB_LE_INPUTS_MUST_BE_BEFORE_PARAMS,      14); //Error in operator '%1%'. All inputs must be before other parameters
ERRCODE(SCIDB_LE_VAR_MUST_BE_AFTER_PARAMS,          15); //Error in operator '%1%'. Variadic parameters must appear last
ERRCODE(SCIDB_LE_UNEXPECTED_GETREPARTSCHEMA,        16); //Unexpected getRepartSchema that is not overwritten
ERRCODE(SCIDB_LE_PWRITE_ERROR,                      17); //pwrite failed to write %1% byte(s) to the position %2% with error %3%"
ERRCODE(SCIDB_LE_PREAD_ERROR,                       18); //pread failed to read %1% byte(s) from position %2% with error %3%
ERRCODE(SCIDB_LE_OPERATOR_NOT_FOUND,                19); //Operator '%1%' not found
ERRCODE(SCIDB_LE_ARRAY_DOESNT_EXIST,                20); //Array '%1%' does not exist
ERRCODE(SCIDB_LE_ARRAYID_DOESNT_EXIST,              21); //Array with id '%1%' does not exist
ERRCODE(SCIDB_LE_CANT_OPEN_FILE,                    22); //Can not open file '%1%'
ERRCODE(SCIDB_LE_INSTANCE_DOESNT_EXIST,             23); //Instance with id %1% does not exist
ERRCODE(SCIDB_LE_ATTRIBUTE_DOESNT_EXIST,            24); //Attribute with id %1% does not exist in array '%2%'
ERRCODE(SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,          25); //Can not create attribute '%1%'; name collides with existing object;
ERRCODE(SCIDB_LE_ILLEGAL_OPERATION,                 26); //Illegal operation: %1%
ERRCODE(SCIDB_LE_UNKNOWN_OPERATION,                 27); //Operation '%1%' for type '%2%' not found
ERRCODE(SCIDB_LE_WORK_QUEUE_FULL,                   28); //Work queue is full
ERRCODE(SCIDB_LE_INVALID_OFFSET_REQUESTED,          29); //Invalid offset requested
ERRCODE(SCIDB_LE_CANT_MERGE_BITMASK,                30); //Cannot merge bitmask with a larger payload
ERRCODE(SCIDB_LE_NOT_IMPLEMENTED,                   31); //Feature '%1%' not yet implemented
ERRCODE(SCIDB_LE_NO_CURRENT_ELEMENT,                32); //No current element
ERRCODE(SCIDB_LE_OPERATION_FAILED,                  33); //Operation '%1%' failed
ERRCODE(SCIDB_LE_UNKNOWN_CONFIG_OPTION,             34); //Unknown config option '%1%'
ERRCODE(SCIDB_LE_ERROR_NEAR_CONFIG_OPTION,          35); //Error '%1%' near config option '%2%'
ERRCODE(SCIDB_LE_ERROR_IN_CONFIGURATION_FILE,       36); //Error '%1%' in configuration file
ERRCODE(SCIDB_LE_FILE_NOT_FOUND,                    37); //File '%1%' not found
ERRCODE(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,        38); //Specified version of array '%1%' does not exist
ERRCODE(SCIDB_LE_CANT_SEND_RECEIVE,                 39); //Can not send or receive network message
ERRCODE(SCIDB_LE_CONNECTION_ERROR,                  40); //Error #%1% when connecting to %2%:%3%
ERRCODE(SCIDB_LE_FUNCTION_NOT_FOUND,                42); //Function '%1%' is not found
ERRCODE(SCIDB_LE_TYPE_CONVERSION_ERROR,             43); //Can not convert type '%1%' into '%2%'
ERRCODE(SCIDB_LE_TYPE_CONVERSION_ERROR2,            44); //Can not convert '%1%' of type '%2%' into '%3%'
ERRCODE(SCIDB_LE_REF_NOT_FOUND,                     45); //Reference '%s' is not found
ERRCODE(SCIDB_LE_ERROR_IN_UDF,                      46); //Error '%1%' in UDF '%2%'
ERRCODE(SCIDB_LE_UNKNOWN_ERROR_IN_UDF,              47); //Unknown error in UDF '%1%'
ERRCODE(SCIDB_LE_TYPE_NOT_REGISTERED,               48); //Type '%1%' is not registered
ERRCODE(SCIDB_LE_TYPE_ALREADY_REGISTERED,           49); //Type '%1%' already registered
ERRCODE(SCIDB_LE_DIMENSION_NOT_EXIST,               50); //Dimension '%1%' does not exist
ERRCODE(SCIDB_LE_ATTRIBUTE_NOT_EXIST,               51); //Attribute '%1%' does not exist
ERRCODE(SCIDB_LE_CANT_FIND_CONVERTER,               52); //Can not find converter from type '%1%' to '%2%'
ERRCODE(SCIDB_LE_CANT_FIND_IMPLICIT_CONVERTER,      53); //Can not find implicit converter from type '%1%' to '%2%'
ERRCODE(SCIDB_LE_PARAMETER_TYPE_ERROR,              55); //Operator expected expression with type '%1%' but passed with type '%2%' which is not convertable
ERRCODE(SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR,  56); //Multi-child operators with specific distribution requirements not supported
ERRCODE(SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2, 57); //Operators with more than two input arrays may only require distribution ANY or COLLOCATED
ERRCODE(SCIDB_LE_LOGICAL_OP_DOESNT_EXIST,           58); //Logical operator '%1%' does not exist
ERRCODE(SCIDB_LE_PHYSICAL_OP_DOESNT_EXIST,          59); //Physical operator '%1%' for logical operator '%2%' does not exist
ERRCODE(SCIDB_LE_LOGICAL_OP_ALREADY_REGISTERED,     60); //Logical operator '%1%' already registered
ERRCODE(SCIDB_LE_PHYSICAL_OP_ALREADY_REGISTERED,    61); //Physical operator '%1%' for logical operator '%2%' already registered
ERRCODE(SCIDB_LE_QUERY_CANCELLED,                    62); //Query %1% was cancelled
ERRCODE(SCIDB_LE_NO_QUORUM,                         63); //Instance liveness has changed
ERRCODE(SCIDB_LE_QUERY_NOT_FOUND,                   64); //Query %1% not found
ERRCODE(SCIDB_LE_QUERY_NOT_FOUND2,                  65); //Query already deallocated/cancelled
ERRCODE(SCIDB_LE_QUERY_WAS_EXECUTED,                66); //Query was executed previously
ERRCODE(SCIDB_LE_MEMORY_ALLOCATION_ERROR,           67); //Error '%1%' during memory allocation
ERRCODE(SCIDB_LE_REDISTRIBUTE_ERROR,                68); //Attempt to redistribute on undefined partitioning schema
ERRCODE(SCIDB_LE_QUERY_PARSING_ERROR,               69); //Query parser failed with error '%1%'
ERRCODE(SCIDB_LE_SUBQUERIES_NOT_SUPPORTED,          70); //Subqueries inside expressions not supported
ERRCODE(SCIDB_LE_WRONG_ASTERISK_USAGE,              71); //Asterisk can be used only in aggregates
ERRCODE(SCIDB_LE_ARRAY_NAME_REQUIRED,               72); //CREATE ARRAY statement required array name
ERRCODE(SCIDB_LE_ARRAY_ALREADY_EXIST,               73); //Array with name '%s' already exist
ERRCODE(SCIDB_LE_DUPLICATE_DIMENSION_NAME,          74); //Can not create dimension '%1%'; name collides with existing object
ERRCODE(SCIDB_LE_REFERENCE_NOT_ALLOWED_IN_DEFAULT,  75); //References not allowed in DEFAULT clause
ERRCODE(SCIDB_LE_NULL_IN_NON_NULLABLE,              76); //Default value for non-nullable attribute '%1%' can not be null
ERRCODE(SCIDB_LE_COMPRESSOR_DOESNT_EXIST,           77); //Compressor with name '%1%' does not exist
ERRCODE(SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT,      78); //Operator '%1%' not expected arguments but passed '%1%'
ERRCODE(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,    79); //Operator '%1%' expected %2% argument(s) but passed %3%
ERRCODE(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2,   80); //Operator '%1%' expected more arguments
ERRCODE(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3,   81); //Operator '%1%' expected only %2% arguments
ERRCODE(SCIDB_LE_WRONG_OPERATOR_ARGUMENT,           82); //Expected '%1%' in position %2% of operator '%3%' but found '%4%'
ERRCODE(SCIDB_LE_NESTED_ARRAYS_NOT_SUPPORTED,       83); //Nested arrays not supported
ERRCODE(SCIDB_LE_NO_MAPPING_ARRAY,                  84); //There is no mapping array for dimensions '%1%' in array '%2%'
ERRCODE(SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,      85); //Parameter of operator is ambiguous
ERRCODE(SCIDB_LE_WRONG_OPERATOR_ARGUMENT2,          86); //Parameter must be %1%
ERRCODE(SCIDB_LE_AMBIGUOUS_ATTRIBUTE,               87); //Attribute '%1%' is ambiguous
ERRCODE(SCIDB_LE_AMBIGUOUS_DIMENSION,               88); //Dimension '%1%' is ambiguous
ERRCODE(SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION, 89); //Array operators can not be used inside scalar expressions
ERRCODE(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,   90); //An aggregate call must have exactly one argument
ERRCODE(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT,          91); //An aggregate call must contain a single attribute reference, SELECT statement or *
ERRCODE(SCIDB_LE_REFERENCE_EXPECTED,                92); //Attribute or dimension reference expected
ERRCODE(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,         93); //Sorting quirks can not be used in expressions
ERRCODE(SCIDB_LE_SELECT_IN_AGGREGATE_WRONG_USAGE,   94); //SciDB supporting only simple SELECT's inside aggregates
ERRCODE(SCIDB_LE_TIMESTAMP_IN_AGGREGATE_WRONG_USAGE,95); //Timestamp can not be used inside aggregate
ERRCODE(SCIDB_LE_UNKNOWN_ARRAY_REFERENCE,           96); //Unknown array reference
ERRCODE(SCIDB_LE_AGGREGATE_EXPECTED,                97); //SELECT list items must contain single aggregate if FROM omitted
ERRCODE(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT2,         98); //An aggregate call must contain a single attribute reference or SELECT statement
ERRCODE(SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED,99); //Input must have single attribute for aggregating in this form
ERRCODE(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,    100); //Attribute or dimension reference '%1%' does not exists
ERRCODE(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,  101); //Attribute or dimension reference '%s' is ambiguous
ERRCODE(SCIDB_LE_INPUT_EXPECTED,                    102); //Array name, array operator or SELECT statement expected
ERRCODE(SCIDB_LE_CAN_NOT_STORE,                     103); //Can not STORE or REDIMENSION_STORE output array into '%1%'. Schemas is not conformant
ERRCODE(SCIDB_LE_AGGREGATE_CANT_BE_NESTED,          104); //Aggregate call can not be nested
ERRCODE(SCIDB_LE_UNEXPECTED_SELECT_INSIDE_AGGREGATE,105); //SELECT inside aggregate not implemented
ERRCODE(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,     106); //SELECT item must be inside aggregate function
ERRCODE(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,    107); //SELECT item must be constant or be inside aggregate function
ERRCODE(SCIDB_LE_WRONG_REGRID_REDIMENSION_SIZES_COUNT,108); //REGRID and WINDOW clause must contain sizes for each dimension
ERRCODE(SCIDB_LE_INJECTED_ERROR,                    109); //Injected error
ERRCODE(SCIDB_LE_ERRNS_CAN_NOT_BE_UNREGISTERED,     110); //Errors namespace '%1%' can not be unregistered
ERRCODE(SCIDB_LE_STORAGE_CLOSE_FAILED,              111); //Storage closed with errors
ERRCODE(SCIDB_LE_UNKNOWN_MESSAGE_TYPE,              112); //Unknown/unexpected message type '%1%'
ERRCODE(SCIDB_LE_IQUERY_PARSER_ERROR,               113); //Iquery parser error: '%1%'
ERRCODE(SCIDB_LE_STORAGE_ALREADY_REGISTERED,        114); //Instance #%1% already registered in system catalog
ERRCODE(SCIDB_LE_STORAGE_NOT_REGISTERED,            115); //Storage is not registered in system catalog
ERRCODE(SCIDB_LE_CANT_ACCEPT_CONNECTION,            116); //Error #%1% (%2%) when accepting connection
ERRCODE(SCIDB_LE_UNKNOWN_MESSAGE_TYPE2,             117); //Invalid message type for ID
ERRCODE(SCIDB_LE_INVALID_SHEDULER_WORK_ITEM,        118); //Invalid work item for Scheduler
ERRCODE(SCIDB_LE_INVALID_SHEDULER_PERIOD,           119); //Invalid period for Scheduler
ERRCODE(SCIDB_LE_CONNECTION_ERROR2,                 120); //Connection error while sending
ERRCODE(SCIDB_LE_CANT_OPEN_PATH,                    121); //Cannot open path '%1%'
ERRCODE(SCIDB_LE_DIRECTORY_EXPECTED,                122); //Path '%1%' is not directory
ERRCODE(SCIDB_LE_BINDING_NOT_SUPPORTED,             123); //This binding not supported
ERRCODE(SCIDB_LE_WRONG_ATTRIBUTE_TYPE,              124); //Attribute '%1%' has incorrect datatype (source: %2%, destination: %3%)
ERRCODE(SCIDB_LE_WRONG_ATTRIBUTE_FLAGS,             125); //Attribute '%1%' has incorrect properties
ERRCODE(SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_TYPE,       126); //Source attribute '%1%' must be of type '%2%'
ERRCODE(SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_FLAGS,      127); //Source attribute '%1%' must not have special properties
ERRCODE(SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_TYPE,  128); //Destination attribute '%1%' must be of type '%2%'
ERRCODE(SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_FLAGS, 129); //Destination attribute '%1%' must not have special properties
ERRCODE(SCIDB_LE_WRONG_SOURCE_DIMENSION_TYPE,       130); //Source dimension '%1%' must be of type '%2%'
ERRCODE(SCIDB_LE_WRONG_DESTINATION_DIMENSION_TYPE,  131); //Destination dimenstion '%1%' must be of type '%2%'
ERRCODE(SCIDB_LE_DIMENSIONS_DONT_MATCH,             132); //Dimensions '%1%' and '%2%' do not match
ERRCODE(SCIDB_LE_UNEXPECTED_DESTINATION_DIMENSION,  133); //No data provided for destination dimension '%1%'
ERRCODE(SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE,  134); //No data provided for destination attribute '%1%'
ERRCODE(SCIDB_LE_WRONG_DIMENSION_TYPE,              135); //Dimension '%1%' has incorrect datatype (source: %2%, destination: %3%)
ERRCODE(SCIDB_LE_AGGREGATE_RESULT_CANT_BE_TRANSFORMED_TO_DIMENSION,136); //Aggregate result '%1%' cannot be transformed into a dimension
ERRCODE(SCIDB_LE_CHUNK_WRONG_ITERATION_MODE,        137); //Operator does not support requested chunk iteration mode
ERRCODE(SCIDB_LE_SUBSTITUTE_ERROR1,                 138); //The substitute array '%1%' must have only one dimension
ERRCODE(SCIDB_LE_SUBSTITUTE_ERROR2,                 139); //The substitute array '%1%' must have only one attribute
ERRCODE(SCIDB_LE_SUBSTITUTE_ERROR3,                 140); //The dimension '%1%' must be integer
ERRCODE(SCIDB_LE_SUBSTITUTE_ERROR4,                 141); //The dimension '%1%' must start at 0
ERRCODE(SCIDB_LE_SUBSTITUTE_ERROR5,                 142); //Input attribute '%1%' does not match the datatype of '%2%'
ERRCODE(SCIDB_LE_LOGICAL_JOIN_ERROR1,               143); //Join must be applied to arrays of same dimensionality
ERRCODE(SCIDB_LE_LOGICAL_JOIN_ERROR2,               144); //Joined arrays has different schemas
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR1,                145); //Multiply only defined for double, float, int64, int32 and int16
ERRCODE(SCIDB_LE_LIST_ERROR1,                       146); //Parameter of 'list' operator can be 'aggregates', 'arrays', 'operators', 'types', 'functions', 'queries', 'libraries', 'instances'
ERRCODE(SCIDB_LE_PATH_IS_EMPTY,                     147); //Given dir path is empty
ERRCODE(SCIDB_LE_CANT_CREATE_DIRECTORY,             148); //Can not create directory '%1%'
ERRCODE(SCIDB_LE_EXPLAIN_ERROR1,                    149); //Operator 'explain_physical' accepts 1 or 2 parameters only
ERRCODE(SCIDB_LE_EXPLAIN_ERROR2,                    150); //Second parameter of 'explain_logical' operator must be 'aql' or 'afl'
ERRCODE(SCIDB_LE_OPERATION_NOT_FOUND,               151); //Operation '%1%' not defined for type '%2%'
ERRCODE(SCIDB_LE_NO_MAPPING_FOR_COORDINATE,         152); //No mapping for such coordinate value
ERRCODE(SCIDB_LE_UNDEFINED_DISTRIBUTION_CANT_HAVE_MAPPER,153); //An undefined distribution cannot have a mapper
ERRCODE(SCIDB_LE_SPECIFIC_DISTRIBUTION_REQUIRED,    154); //Specific requirements must be provided IFF required distribution is specific
ERRCODE(SCIDB_LE_CANT_GET_SYSTEM_TIME,              155); //Cannot get system time
ERRCODE(SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR,   156); //Timer (expires_from_now) returned unexpected error '%1%'
ERRCODE(SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR2,  157); //Timer handler returned unexpected error '%1%'
ERRCODE(SCIDB_LE_CANT_LINK_DATA_TO_ZERO_BUFFER,     158); //Cannot link data to zero length buffer
ERRCODE(SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA,159); //Cannot modify Value object with linked data
ERRCODE(SCIDB_LE_CANT_SET_VALUE_VECTOR_TO_DEFAULT,  160); //Cannot set Value vector to default
ERRCODE(SCIDB_LE_CANT_ADD_NULL_FACTORY,             161); //Cannot add NULL factory to Aggregate Library
ERRCODE(SCIDB_LE_DUPLICATE_AGGREGATE_FACTORY,       162); //Cannot add duplicate factory to Aggregate Library
ERRCODE(SCIDB_LE_CANT_INCREMENT_LOCK,               163); //Can not increment lock counter because query was canceled
ERRCODE(SCIDB_LE_DIMENSIONS_MISMATCH,               164); //Dimensions mismatch
ERRCODE(SCIDB_LE_CHUNK_ALREADY_EXISTS,              165); //Update of existed chunk is not possible
ERRCODE(SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE,166); //Merge of chunks with varying size attribute type is not supported
ERRCODE(SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK,       167); //Can not update read-only chunk
ERRCODE(SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK,168); //Type mismatch between aggregate and chunk
ERRCODE(SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE,  169); //Aggregate state must be nullable
ERRCODE(SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE,170); //Extract attribute should be fixed size
ERRCODE(SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE,171); //Extract attribute can not have boolean type
ERRCODE(SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS,        172); //Wrong number of dimensions
ERRCODE(SCIDB_LE_UNALIGNED_COORDINATES,             173); //Coordinates should be aligned on chunk boundary
ERRCODE(SCIDB_LE_CANT_ALLOCATE_MEMORY,              174); //Failed to allocate memory
ERRCODE(SCIDB_LE_NO_CURRENT_CHUNK,                  175); //No current chunk
ERRCODE(SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES,           176); //Chunk is out of array boundaries
ERRCODE(SCIDB_LE_ACCESS_TO_EMPTY_CELL,              177); //Access to empty cell
ERRCODE(SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE,    178); //Assigning NULL to non-nullable attribute
ERRCODE(SCIDB_LE_CANT_USE_TILE_MODE_FOR_APPEND,     179); //Can not use tile mode for append
ERRCODE(SCIDB_LE_TILE_NOT_ALIGNED,                  180); //Not aligned tile position
ERRCODE(SCIDB_LE_TILE_MODE_EXPECTED_STRIDE_MAJOR_ORDER,181); //Tile can be written only in stride major order
ERRCODE(SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE,182); //Invalid operation for sequential mode
ERRCODE(SCIDB_LE_CANT_UPDATE_BITMAP_IN_TILE_MODE,   183); //Can not update bitmap in tile mode
ERRCODE(SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK,        184); //No associated bitmap chunk
ERRCODE(SCIDB_LE_NO_CURRENT_BITMAP_CHUNK,           185); //No current bitmap chunk
ERRCODE(SCIDB_LE_CANT_FETCH_CHUNK_BODY,             186); //Failed to fetch chunk body
ERRCODE(SCIDB_LE_STRIDE_SHOULD_BE_BYTE_ALIGNED,     187); //Stride should be byte aligned
ERRCODE(SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED,188); //Multidimensional array is not allowed
ERRCODE(SCIDB_LE_NO_DATA_FOR_INITIALIZE,            189); //Nothing to initialize data from
ERRCODE(SCIDB_LE_COMPRESSION_DESTINATION_BITMAP_NOT_EMPTY,190); //Compression destination bitmap not empty
ERRCODE(SCIDB_LE_OP_SAMPLE_ERROR1,                  191); //Seed should be positive
ERRCODE(SCIDB_LE_OP_SAMPLE_ERROR2,                  192); //Probability should belong to (0..1] interval
ERRCODE(SCIDB_LE_INVALID_INSTANCE_ID,                   193); //Invalid instance ID: '%1%'
ERRCODE(SCIDB_LE_ARRAYS_NOT_CONFORMANT,             194); //Arrays are not conformant
ERRCODE(SCIDB_LE_UNTERMINATED_CHARACTER_CONSTANT,   195); //Unterminated character constant
ERRCODE(SCIDB_LE_UNTERMINATED_STRING_LITERAL,       196); //Unterminated string literal
ERRCODE(SCIDB_LE_BAD_LITERAL,                       197); //Bad literal
ERRCODE(SCIDB_LE_OP_STORE_ERROR1,                   198); //Arrays with user defined coordinates can be updated only using REDIMENSION_STORE
ERRCODE(SCIDB_LE_OP_REDIMENSION_ERROR1,             199); //Destination for REDIMENSION should be emptyable array
ERRCODE(SCIDB_LE_NO_MEMORY_TO_ALLOCATE_MATRIX,      200); //Not enough memory to allocate matrix
ERRCODE(SCIDB_LE_NO_MEMORY_TO_ALLOCATE_VECTOR,      201); //Not enough memory to allocate vector
ERRCODE(SCIDB_LE_CANT_UPDATE_CHUNK,                 202); //Can not update disk chunk
ERRCODE(SCIDB_LE_INVALID_REDUNDANCY,                203); //Redundancy should be smaller than number of instances
ERRCODE(SCIDB_LE_INSTANCE_OFFLINE,                      204); //Instance #%1% is offline
ERRCODE(SCIDB_LE_DIVISION_BY_ZERO,                  205); //Division by zero
ERRCODE(SCIDB_LE_FAILED_PARSE_STRING,               206); //Failed to parse string
ERRCODE(SCIDB_LE_DIMENSION_EXPECTED,                207); //Dimension has to be specified
ERRCODE(SCIDB_LE_CANT_CONVERT_ARRAY_TO_SCALAR,      208); //Array can not be converted to scalar
ERRCODE(SCIDB_LE_REDISTRIBUTE_AGGREGATE_ERROR1,     209); //Empty tag attribute should be last and can not be aggregated
ERRCODE(SCIDB_LE_REDISTRIBUTE_ERROR1,               210); //Empty tag attribute should be last
ERRCODE(SCIDB_LE_CANT_MERGE_READONLY_CHUNK,         211); //Can not merge read-only chunk
ERRCODE(SCIDB_LE_TOO_MANY_COORDINATES,              212); //Too much coordinate values for the specified dimension
ERRCODE(SCIDB_LE_CANT_REDIMENSION_NULL,             213); //Null can not be redimensioned
ERRCODE(SCIDB_LE_INVALID_SPECIFIED_DATE,            214); //Specified date is not valid
ERRCODE(SCIDB_LE_CANT_CONVERT_NULL,                 215); //Can not convert NULL value
ERRCODE(SCIDB_LE_NO_QUORUM2,                        216); //Query can not be executed since there is not enough online instances
ERRCODE(SCIDB_LE_OP_REGRID_ERROR1,                  217); //Regrid interval should be positive
ERRCODE(SCIDB_LE_OP_WINDOW_ERROR1,                  218); //Window size should be positive
ERRCODE(SCIDB_LE_OP_WINDOW_ERROR2,                  219); //Window doesn't fit in overlap area
ERRCODE(SCIDB_LE_OP_BUILD_ERROR1,                   220); //Build expression should not access attributes
ERRCODE(SCIDB_LE_OP_BUILD_ERROR2,                   221); //Constructed array should have one attribute
ERRCODE(SCIDB_LE_OP_BUILD_ERROR3,                   222); //Array with open boundary can not be built
ERRCODE(SCIDB_LE_OP_BUILD_SPARSE_ERROR1,            223); //Build_sparse expression should not access attributes
ERRCODE(SCIDB_LE_OP_BUILD_SPARSE_ERROR2,            224); //Build_sparse predicate should not access attributes
ERRCODE(SCIDB_LE_OP_BUILD_SPARSE_ERROR3,            225); //Constructed array should have one attribute
ERRCODE(SCIDB_LE_OP_BUILD_SPARSE_ERROR4,            226); //Array with open boundary can not be built
ERRCODE(SCIDB_LE_OP_CAST_ERROR1,                    227); //Mismatched number of attributes
ERRCODE(SCIDB_LE_OP_CAST_ERROR2,                    228); //Mismatched attributes types
ERRCODE(SCIDB_LE_OP_CAST_ERROR3,                    229); //Mismatched attributes flags
ERRCODE(SCIDB_LE_OP_CAST_ERROR4,                    230); //Mismatched number of dimensions
ERRCODE(SCIDB_LE_OP_CAST_ERROR5,                    231); //Dimension length doesn't match
ERRCODE(SCIDB_LE_OP_CAST_ERROR6,                    232); //Dimension start doesn't match
ERRCODE(SCIDB_LE_OP_CAST_ERROR7,                    233); //Dimension chunk interval doesn't match
ERRCODE(SCIDB_LE_OP_CAST_ERROR8,                    234); //Dimension chunk overlap doesn't match
ERRCODE(SCIDB_LE_OP_CONCAT_ERROR1,                  235); //Arrays with open boundary can not be concatenated
ERRCODE(SCIDB_LE_OP_CROSSJOIN_ERROR1,               236); //Dimension should be specified only once in JOIN ON list
ERRCODE(SCIDB_LE_OP_DELDIM_ERROR1,                  237); //Can not delete last dimension of array
ERRCODE(SCIDB_LE_OP_DELDIM_ERROR2,                  238); //Only dimension with length 1 can be removed
ERRCODE(SCIDB_LE_OP_REDIMENSION_ERROR2,             239); //The result of an aggregate cannot be transformed into a dimension
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR1,       240); //Resulting array has no dimensions
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR2,       241); //Destination for REDIMENSION should be emptyable array
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR3,       242); //REDIMENSION_STORE can not fill overlap area
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR4,       243); //Dimension '%1%' is not compatible
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR5,       244); //Attribute '%1%' cannot be transformed into a dimension
ERRCODE(SCIDB_LE_OP_INPUT_ERROR1,                   245); //Can not jump on more than one chunk forward
ERRCODE(SCIDB_LE_OP_INPUT_ERROR2,                   246); //Input parser expected '%1%'
ERRCODE(SCIDB_LE_OP_INPUT_ERROR3,                   247); //Input parser expected coordinate
ERRCODE(SCIDB_LE_OP_INPUT_ERROR4,                   248); //Invalid chunk position
ERRCODE(SCIDB_LE_OP_INPUT_ERROR5,                   249); //Duplicate chunk address
ERRCODE(SCIDB_LE_OP_INPUT_ERROR6,                   250); //Too much nesting
ERRCODE(SCIDB_LE_OP_INPUT_ERROR7,                   251); //Out of bounds
ERRCODE(SCIDB_LE_OP_INPUT_ERROR8,                   252); //Input parser expected literal
ERRCODE(SCIDB_LE_OP_INPUT_ERROR9,                   253); //No values for empty cells should be defined: only () is accepted
ERRCODE(SCIDB_LE_OP_INPUT_ERROR10,                  254); //Invalid format of input file
ERRCODE(SCIDB_LE_OP_INPUT_ERROR11,                  255); //Chunk out of window
ERRCODE(SCIDB_LE_OP_INPUT_ERROR12,                  256); //Unterminated character constant
ERRCODE(SCIDB_LE_OP_INPUT_ERROR13,                  257); //Unterminated string literal
ERRCODE(SCIDB_LE_OP_INPUT_ERROR14,                  258); //Bad literal
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR1,                 259); //This matrix cannot be inverted
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR2,                 260); //Determinant = 0.0, this matrix cannot be inverted
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR3,                 261); //Matrix must contain 1 attribute
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR4,                 262); //Matrix must contain attribute of double type
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR5,                 263); //Only 2-D matrices can be inverted
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR6,                 264); //Inversion only works on square matrices
ERRCODE(SCIDB_LE_OP_INVERSE_ERROR7,                 265); //Cannot invert an unbounded matrix
ERRCODE(SCIDB_LE_NO_CURRENT_POSITION,               266); //No current position
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR2,                267); //Matrix must contain 1 attrbiute
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR3,                268); //Array must be rectangular matrix
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR4,                269); //Cannot multiply unbounded matrices
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR5,                270); //Matrix dimensions must match
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR6,                271); //Matrix chunk intervals in the joint dimension must match
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR7,                272); //Multipled matrices should have the same attribute type
ERRCODE(SCIDB_LE_OP_MULTIPLY_ERROR8,                273); //Matrices with nullable attributes can not be multiplied
ERRCODE(SCIDB_LE_OPERATION_NOT_FOUND2,              274); //Operation '%1%' not defined for types '%2%' and '%3%'
ERRCODE(SCIDB_LE_OP_NORMALIZE_ERROR1,               275); //Vector must contain 1 attrbiute
ERRCODE(SCIDB_LE_OP_NORMALIZE_ERROR2,               276); //Array must be one dimensional vector
ERRCODE(SCIDB_LE_OP_REPART_ERROR1,                  277); //Mismatched number of dimensions
ERRCODE(SCIDB_LE_OP_REPART_ERROR2,                  278); //Mismatched array coordinate type
ERRCODE(SCIDB_LE_OP_REPART_ERROR3,                  279); //Mismatched array base
ERRCODE(SCIDB_LE_OP_REPART_ERROR4,                  280); //Mismatched array length
ERRCODE(SCIDB_LE_OP_REPART_ERROR5,                  281); //Array should have fixed low boundary
ERRCODE(SCIDB_LE_OP_RESHAPE_ERROR1,                 282); //Size of array should be fixed
ERRCODE(SCIDB_LE_OP_RESHAPE_ERROR2,                 283); //Chunk should have no overlap
ERRCODE(SCIDB_LE_OP_RESHAPE_ERROR3,                 284); //Incompatible array dimensions
ERRCODE(SCIDB_LE_OP_REVERSE_ERROR1,                 285); //Only fixed size array can be reversed
ERRCODE(SCIDB_LE_OP_SLICE_ERROR1,                   286); //Input array has too few dimensions for slice
ERRCODE(SCIDB_LE_OP_SLICE_ERROR2,                   287); //Slice coordinate is out of bounds
ERRCODE(SCIDB_LE_OP_SORT_ERROR2,                    289); //Column index is out of range
ERRCODE(SCIDB_LE_OP_SORT_ERROR3,                    290); //Can not jump on more than one chunk forward
ERRCODE(SCIDB_LE_OP_SORT_ERROR4,                    291); //Chunk out of window
ERRCODE(SCIDB_LE_OP_THIN_ERROR1,                    292); //Step must be divider of chunk size
ERRCODE(SCIDB_LE_OP_THIN_ERROR2,                    293); //Starting position is out of bounds
ERRCODE(SCIDB_LE_OP_THIN_ERROR3,                    294); //Starting offset must be smaller than step
ERRCODE(SCIDB_LE_OP_THIN_ERROR4,                    295); //Step must be smaller than chunk size
ERRCODE(SCIDB_LE_OP_TRANSPOSE_ERROR1,               296); //Cannot transpose an unbounded array
ERRCODE(SCIDB_LE_OP_UNPACK_ERROR1,                  297); //Size of of array is not multiple of chunk interval
ERRCODE(SCIDB_LE_OP_XGRID_ERROR1,                   298); //Cannot xrid an unbounded array
ERRCODE(SCIDB_LE_OP_XGRID_ERROR2,                   299); //Xgrid arguments must be positive
ERRCODE(SCIDB_LE_DIMENSION_START_CANT_BE_UNBOUNDED, 300); //Dimension start cannot be unbounded
ERRCODE(SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW,         301); //High should not be less than low
ERRCODE(SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,      302); //Overlap can not be larger than chunk interval
ERRCODE(SCIDB_LE_DIMENSIONS_NOT_SPECIFIED,          303); //Must specify at least one element in the array
ERRCODE(SCIDB_LE_PIN_UNPIN_DISBALANCE,              304); //pin/unpin disbalance or race condition during close
ERRCODE(SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED,       305); //Compression method is not defined for the chunk
ERRCODE(SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT, 307); //Invalid format of storage description file
ERRCODE(SCIDB_LE_CHUNK_NOT_FOUND,                   308); //Chunk not found
ERRCODE(SCIDB_LE_NO_FREE_SPACE,                     309); //No more free space
ERRCODE(SCIDB_LE_CHUNK_SIZE_TOO_LARGE,              310); //Chunk size should not be large than cluster size
ERRCODE(SCIDB_LE_CANT_REALLOCATE_MEMORY,            311); //Failed to reallocate memory
ERRCODE(SCIDB_LE_INVALID_LIVENESS,                  312); //Invalid liveness
ERRCODE(SCIDB_LE_LIVENESS_MISMATCH,                 313); //Liveness mismatch
ERRCODE(SCIDB_LE_MESSAGE_MISSED_QUERY_ID,           314); //Message has no query ID
ERRCODE(SCIDB_LE_VARIABLE_SIZE_TYPES_NOT_SUPPORTED_BY_VAR,315); //Variable size types not supported by var
ERRCODE(SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES,316); //Mismatched coordinates in physical boundaries
ERRCODE(SCIDB_LE_DIMENSIONS_DONT_MATCH_COORDINATES, 317); //Dimensions don't match coordinates
ERRCODE(SCIDB_LE_MISMATCHED_BOUNDARIES,             318); //Mismatched boundaries
ERRCODE(SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY,319); //Can not to create boundaries from infinite array
ERRCODE(SCIDB_LE_LIVENESS_EMPTY,                    320); //Liveness is empty
ERRCODE(SCIDB_LE_DUPLICATE_QUERY_ID,                321); //Query with such ID already exists in the system
ERRCODE(SCIDB_LE_CANT_ACQUIRE_READ_LOCK,            322); //Failed to acquire a read lock
ERRCODE(SCIDB_LE_CANT_GENERATE_UTC_TIME,            323); //Unable to generate UTC time
ERRCODE(SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION,324); //Can not to create an SG with undefined distribution
ERRCODE(SCIDB_LE_MALFORMED_AGGREGATE,               325); //Malformed or unsupported aggregate
ERRCODE(SCIDB_LE_CANT_LOCK_DATABASE,                326); //Failed to get exclusive access to the database
ERRCODE(SCIDB_LE_DATABASE_HEADER_CORRUPTED,         327); //Database header is corrupted
ERRCODE(SCIDB_LE_CANT_DECOMPRESS_CHUNK,             328); //Failed to decompress chunk
ERRCODE(SCIDB_LE_CHUNK_NOT_PINNED,                  329); //Accessed chunk should be pinned
ERRCODE(SCIDB_LE_CHUNK_FILE_HAS_INVALID_NAME,       330); //Chunk file has invalid name
ERRCODE(SCIDB_LE_DFS_DIRECTORY_NOT_EXIST,           331); //DFS directory doesn not exist
ERRCODE(SCIDB_LE_INVALID_MONTH_REPRESENTATION,      332); //Invalid month representation: %1%
ERRCODE(SCIDB_LE_INVALID_USER_ERROR_CODE,           333); //Can not register errors namespace '%1%' with error code '%2%': user defined error codes must be greater than %3%. Use constant SCIDB_USER_ERROR_CODE_START to define your first error code.
ERRCODE(SCIDB_LE_ATTRIBUTES_MISMATCH,               334); //Attributes mismatch
ERRCODE(SCIDB_LE_INVALID_FUNCTION_ARGUMENT,         335); //Invalid function argument: %1%
ERRCODE(SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE,      336); //Chunk size should not exceed 2^64
ERRCODE(SCIDB_W_FILE_NOT_FOUND_ON_INSTANCES,            337); //File '%1%' not found on instance(s) %2%
ERRCODE(SCIDB_LE_FILE_IMPORT_FAILED,                338); //Import from file '%1%' (instance %2%) to array '%3%' failed at line %4%, column %5%, offset %6%, value='%7%': %8%
ERRCODE(SCIDB_LE_ACCESS_TO_RAW_CHUNK,               339); //Failed to fetch chunk body
ERRCODE(SCIDB_LE_INVALID_COMMIT_STATE,              340); //'%2' request for query (%1) is invalid in the current state
ERRCODE(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST2,       341); //Array '%1%' is immutable and dont has versions
ERRCODE(SCIDB_LE_WRONG_ASTERISK_USAGE2,             342); //Asterisk for all array versions access can be used only in SELECT
ERRCODE(SCIDB_LE_LE_CANT_ACCESS_INDEX_FOR_ALLVERSIONS,343); //Only index of specified array version can be accessed
ERRCODE(SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,         344); //Array version inaccessible in this context
ERRCODE(SCIDB_LE_CANT_ACCESS_INDEX_ARRAY,           345); //Index array inaccessible in this context
ERRCODE(SCIDB_LE_OP_THIN_ERROR5,                    346); //Step must be positive number
ERRCODE(SCIDB_LE_CANT_UNLOAD_MODULE,                347); //Can not unload module '%1%'. This module is not loaded.
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR6,       348); //Aggregate '%1%' doesn't match with any attribute
ERRCODE(SCIDB_LE_SUBSTITUTE_FAILED,                 349); //Failed to substitute missing reason '%1%'
ERRCODE(SCIDB_LE_REMOVE_NOT_POSSIBLE,               350); //Remove of array is not possible because it is still in use
ERRCODE(SCIDB_LE_TRUNCATION,                        351); //Varying size type with length %1% can not be converted to fixed size type %2% 
ERRCODE(SCIDB_LE_LOOKUP_BAD_PARAM,                  352); //Number of attributes in pattern array of LOOKUP smatch number of dimensions in source array 
ERRCODE(SCIDB_LE_INVALID_STORAGE_HEADER,            353); //Invalid storage header format
ERRCODE(SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION, 354); //Mismatched storage format version: %1% instead of %2% required
ERRCODE(SCIDB_LE_OP_CROSSJOIN_ERROR2,               355); //Joined dimensions should be specified for both arrays
ERRCODE(SCIDB_LE_OP_NORMALIZE_ERROR3,               356); //Attribute must be of double type
ERRCODE(SCIDB_LE_EXPLICIT_EMPTY_FLAG_NOT_ALLOWED,   357); //Explicit empty flag attribute not allowed. Use EMPTY keyword.
ERRCODE(SCIDB_LE_OP_CAST_ERROR9,                    358); //Dimension type doesn't match
ERRCODE(SCIDB_LE_INCONSISTENT_ARRAY_DESC,           359); //Array descriptor is inconsistent
ERRCODE(SCIDB_LE_FUNC_MAP_TRANSFORMATION_NOT_POSSIBLE, 360); //Not able to perform requested functional mapping transformation
ERRCODE(SCIDB_LE_OP_REDIMENSION_STORE_ERROR7,       361); //Too much duplicates for REDIMENSION_STORE with new dimension: increase chunk size
ERRCODE(SCIDB_LE_QUERY_ALREADY_COMMITED,            362); //Operation not possible because the query (%1%) already commited
ERRCODE(SCIDB_LE_CAN_NOT_CHANGE_MAPPING,            363); //Can not change coordinates mapping for non-versioned array %1%
ERRCODE(SCIDB_LE_NETWORK_QUEUE_FULL,                364); //Network queue is full
ERRCODE(SCIDB_LE_INVALID_MESSAGE_FORMAT,            365); //Invalid message format for type %1%
ERRCODE(SCIDB_LE_INVALID_ARRAY_LITERAL,             366); //Build required constant string when array literal flag is true
ERRCODE(SCIDB_LE_INVALID_REDIMENSION_POSITION,      367); //Cannot redimension coordinates %1%; the position is not valid in the destination array
ERRCODE(SCIDB_LE_SYSCALL_ERROR,                     368); //Invocation of %1% failed with return code=%2%, error=%3%, arg(s)=%4%
ERRCODE(SCIDB_LE_NON_FQ_PATH_ERROR,                 369); //Path %1% is not fully qualified
ERRCODE(SCIDB_LE_UNKNOWN_CTX,                       370); //Unknown/unexpected context type '%1%'
ERRCODE(SCIDB_LE_CANT_LOCK_FILE,                    371); //Failed to get exclusive access to the file %1%
ERRCODE(SCIDB_LE_TEMPLATE_PARSE_ERROR,              372); //Failed to parser templace at position %1%
ERRCODE(SCIDB_LE_TEMPLATE_FIXED_SIZE_TYPE,          373); //Length should not be explcitly specified for fixed size type %1%
ERRCODE(SCIDB_LE_FILE_WRITE_ERROR,                  374); //Failed to write file: %1%
ERRCODE(SCIDB_LE_FILE_READ_ERROR,                   375); //Failed to read file: %1%
ERRCODE(SCIDB_LE_UNSUPPORTED_FORMAT,                376); //Unsupported format: %1%
ERRCODE(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,  377); //Same dimension specified many times
ERRCODE(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION,378); //Not all dimensions were specified
ERRCODE(SCIDB_LE_PARTITION_NAME_NOT_UNIQUE,         379); //Partition name should be unique
ERRCODE(SCIDB_LE_PARTITION_NAME_NOT_SPECIFIED,      380); //Partition name not specified
ERRCODE(SCIDB_LE_UNKNOWN_PARTITION_NAME,            381); //Unknown partition name
ERRCODE(SCIDB_LE_OP_WINDOW_ERROR3,                  382); //PRECEDING and FOLLOWING must be zero or positive values
ERRCODE(SCIDB_LE_WRONG_OVER_USAGE,                  383); //OVER keyword can be applied only to aggregates
ERRCODE(SCIDB_W_MISSING_REASON_OUT_OF_BOUNDS,       384); //Missing reason can not be imported in binary file
ERRCODE(SCIDB_LE_DDL_CANT_BE_NESTED,                385); //DDL operator can not be nested
ERRCODE(SCIDB_LE_NO_OPERATOR_RESULT,                386); //Nested operator not returned any result to parent
ERRCODE(SCIDB_LE_CATALOG_NEWER_THAN_SCIDB,          387); //Can not connect SciDB with metadata version %1% to catalog with metadata version %2%
ERRCODE(SCIDB_LE_CATALOG_METADATA_UPGRADE_ERROR,    388); //Catalog metadata upgrade failed with error: %1%
ERRCODE(SCIDB_LE_DDL_SHOULDNT_HAVE_INPUTS,          389); //DDL operator should not have inputs
ERRCODE(SCIDB_LE_UNSUPPORTED_INPUT_ARRAY,           390); //Operator %1 does not support the given virtual input array. Consider splitting the query and using store() to create a temporary result
ERRCODE(SCIDB_LE_OP_WINDOW_ERROR4,                  391); //Window size should be greater than 1
ERRCODE(SCIDB_LE_OP_INPUT_ERROR15,                  392); //Expected boolean constant for empty indicator
ERRCODE(SCIDB_LE_OP_INPUT_ERROR16,                  393); //Number of failures exceeds threshold
ERRCODE(SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE,       394); //Chunk size must be positive
ERRCODE(SCIDB_LE_OPERATION_FAILED_WITH_ERRNO,       395); //Operation %1% failed with errno %2%
ERRCODE(SCIDB_LE_OP_RLE_EXPECTED,                   396); //Chunk expected to be in RLE format
ERRCODE(SCIDB_LE_OP_NONEMPTY_EXPECTED,              397); //Non-emptyable array expected
//Next long ERRCODE

ERRCODE(SCIDB_LE_PG_QUERY_EXECUTION_FAILED,         1001); //Execution of query '%1%' failed with error %2%
ERRCODE(SCIDB_LE_LIBPQ_NOT_THREADSAFE,              1002); //libpq is not built as threadsafe, rebuild it with --enable-thread-safety
ERRCODE(SCIDB_LE_CANT_CONNECT_PG,                   1003); //Can not connect to PostgreSQL catalog: '%1'
ERRCODE(SCIDB_LE_UNREACHABLE_CODE,                  1004); //Fatal: Unreachable code is reached in '%1%'

// copied from P4
ERRCODE(SCIDB_LE_DLA_ERROR13,               2001); //Request for unknown attribute
ERRCODE(SCIDB_LE_DLA_ERROR14,               2002); //Specified attribute not found in array
ERRCODE(SCIDB_LE_DLA_ERROR15,               2003); //Ranked attribute cannot be an empty indicator
ERRCODE(SCIDB_LE_DLA_ERROR16,               2004); //Specified dimension not found in array
ERRCODE(SCIDB_LE_DLA_ERROR17,               2005); //The number of samples passed to quantile must be at least 1

} //namespace scidb

#endif /* ERRORCODES_H_ */
