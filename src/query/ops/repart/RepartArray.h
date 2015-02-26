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
 * @file RepartArray.cpp
 *
 * @brief Repart array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef REPART_ARRAY_H
#define REPART_ARRAY_H

#include "system/Config.h"
#include "array/Array.h"

namespace scidb {

Array* createRepartArray(ArrayDesc const& desc,
                         boost::shared_ptr<Array> const& array,
                         const boost::shared_ptr<Query>& query,
                         RepartAlgorithm const& algorithm,
                         bool denseOpenOnce,
                         bool outputFullChunks,
                         bool singleInstance,
                         bool tileMode,
                         uint64_t sequenceScanThreshold);
} /* namespace scidb */

#endif
