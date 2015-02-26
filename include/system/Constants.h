/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * @file 
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief This file contains ONLY externs to source-wide constants. If you need add/change constants
 *        edit file Constants.cpp.in
 */

#ifndef CONSTANTS_H_
#define CONSTANTS_H_

#include <stdint.h>
#include <string>

namespace scidb
{
const char* SCIDB_VERSION();        /// @brief The full version string     : maj.min.pat.bld
const char* SCIDB_VERSION_PUBLIC(); /// @brief The public version string   : maj.min.bld
const char* SCIDB_BUILD_TYPE();     /// @brief The build type              : release|debug|...
const char* SCIDB_COPYRIGHT();      /// @brief The offical copyright string: Copyright (C) 2008-...

uint32_t    SCIDB_VERSION_MAJOR();  /// @brief The major revision number
uint32_t    SCIDB_VERSION_MINOR();  /// @brief The minor revision number
uint32_t    SCIDB_VERSION_PATCH();  /// @brief The patch revision number
uint32_t    SCIDB_VERSION_BUILD();  /// @brief The build revision number

const char* SCIDB_INSTALL_PREFIX();

std::string SCIDB_BUILD_INFO_STRING(const char* separator = "\n");

std::string DEFAULT_MPI_DIR();
std::string DEFAULT_MPI_TYPE();

extern const size_t MB;
extern const size_t GB;

extern const size_t DEFAULT_MEM_THRESHOLD;
extern const double DEFAULT_DENSE_CHUNK_THRESHOLD;
extern const double DEFAULT_SPARSE_CHUNK_INIT_SIZE;
extern const int    DEFAULT_STRING_SIZE_ESTIMATION;
extern const int    METADATA_VERSION;

/**
 * The maximum number of dimensions that SciDB supports.
 * This constant is needed at compile time; so it cannot be set in Constants.cpp.in.
 */
const size_t MAX_NUM_DIMS_SUPPORTED = 100;
}

#endif /* CONSTANTS_H_ */
