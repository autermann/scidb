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
 * @file
 *
 * @brief Routines for serializing physical plans to strings.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SERIALIZE_H_
#define SERIALIZE_H_

#include <string>

#include "query/QueryPlan.h"

namespace scidb
{

std::string serializePhysicalPlan(const boost::shared_ptr<PhysicalPlan> &plan);

string serializePhysicalExpression(const Expression &expr);

Expression deserializePhysicalExpression(const string &str);

}
#endif /* SERIALIZE_H_ */
