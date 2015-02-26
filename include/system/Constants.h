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

const char* SCIDB_VERSION();
const char* SCIDB_BUILD_TYPE();

uint32_t SCIDB_VERSION_MAJOR();
uint32_t SCIDB_VERSION_MINOR();
uint32_t SCIDB_VERSION_PATCH();
uint32_t SCIDB_VERSION_BUILD();

const char* SCIDB_VERSION_CODENAME();

const char* SCIDB_INSTALL_PREFIX();

std::string SCIDB_BUILD_INFO_STRING(const char* separator = "\n");

extern const size_t MB;
extern const size_t GB;

extern const size_t DEFAULT_MEM_THRESHOLD;
extern const double DEFAULT_DENSE_CHUNK_THRESHOLD;
extern const double DEFAULT_SPARSE_CHUNK_INIT_SIZE;
extern const int DEFAULT_STRING_SIZE_ESTIMATION;

extern const int METADATA_VERSION;

/**
 * The maximum number of dimensions that SciDB supports.
 * This constant is needed at compile time; so it cannot be set in Constants.cpp.in.
 */
const size_t MAX_NUM_DIMS_SUPPORTED = 100;
}

#endif /* CONSTANTS_H_ */
