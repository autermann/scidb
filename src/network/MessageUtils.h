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
 * @file MessageUtils.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef MESSAGEUTILS_H_
#define MESSAGEUTILS_H_

#include <boost/shared_ptr.hpp>

#include "network/BaseConnection.h"
#include "array/Array.h"
#include "system/Exceptions.h"
#include "query/Query.h"

namespace scidb
{

#ifndef SCIDB_CLIENT

boost::shared_ptr<MessageDesc> makeErrorMessageFromException(const Exception& e, QueryID queryID = 0);
boost::shared_ptr<MessageDesc> makeErrorMessage(int code, const std::string& errorMessage, QueryID queryID);
boost::shared_ptr<MessageDesc> makeOkMessage(QueryID queryID = 0);
boost::shared_ptr<MessageDesc> makeAbortMessage(QueryID queryID);
boost::shared_ptr<MessageDesc> makeCommitMessage(QueryID queryID);

bool parseQueryLiveness(boost::shared_ptr<NodeLiveness>& queryLiveness,
                        boost::shared_ptr<scidb_msg::PhysicalPlan>& ppMsg);

bool serializeQueryLiveness(boost::shared_ptr<const NodeLiveness>& queryLiveness,
                            boost::shared_ptr<scidb_msg::PhysicalPlan>& ppMsg);

#endif //SCIDB_CLIENT

boost::shared_ptr<Exception> makeExceptionFromErrorMessage(const boost::shared_ptr<MessageDesc> &msg);

void makeExceptionFromErrorMessageAndThrow(const boost::shared_ptr<MessageDesc> &msg);
} // namespace

#endif /* MESSAGEUTILS_H_ */
