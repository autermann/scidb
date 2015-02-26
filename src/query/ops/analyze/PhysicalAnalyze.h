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
 * PhysicalAnalyze.h
 *
 *  Created on: Feb 14, 2012
 *      Author: egor.pugin@gmail.com
 */

#ifndef _PHYSICAL_ANALYZE_H_
#define _PHYSICAL_ANALYZE_H_

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/FileArray.h"
#include "query/Network.h"

//misc
#define ANALYZE_CHUNK_SIZE          1000
#define ANALYZE_ATTRIBUTES          5

//limits
#define ANALYZE_MAX_MEMORY_PER_THREAD_BYTES 1024 * 1024
#define ANALYZE_MAX_PRECISE_COUNT   4000

namespace scidb {

using namespace boost;
using namespace std;

struct AnalyzeData
{
    string attribute_name;
    string min;
    string max;
    uint64_t distinct_count;
    uint64_t non_null_count;

    AnalyzeData()
        : distinct_count(0), non_null_count(0)
    {
    }
};

class PhysicalAnalyze : public PhysicalOperator
{
private:
    //hash functions
    inline uint32_t fnv1a32(uint8_t *value, size_t size);

    //current hash
    inline uint64_t hash(uint8_t *value, size_t size);
    inline uint64_t hash(uint64_t value);
public:
    PhysicalAnalyze(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema);

    boost::shared_ptr<Array> execute(vector<boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query);

    void analyzeBuiltInType(AnalyzeData *data, boost::shared_ptr<ConstArrayIterator> arrIt, TypeId typeId, boost::shared_ptr<Query> query);
    void analyzeStringsAndUDT(AnalyzeData *data, boost::shared_ptr<ConstArrayIterator> arrIt, TypeId typeId, boost::shared_ptr<Query> query);
};

}  // namespace scidb

#endif
