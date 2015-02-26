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
 * @file LogicalAllVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of updatable array versions
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

using namespace std;
using namespace boost;

class LogicalAllVersions: public LogicalOperator
{
public:
    LogicalAllVersions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_IN_ARRAY_NAME()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        size_t nAllVersions = SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()).size();
        Dimensions const& srcDims = arrayDesc.getDimensions();
        size_t nDims = srcDims.size();
        Dimensions dstDims(nDims+1);
        dstDims[0] = DimensionDesc("VersionNo", 1, 1, nAllVersions, nAllVersions, 1, 0);
        for (size_t i = 0; i < nDims; i++) { 
            DimensionDesc const& dim = srcDims[i];
            dstDims[i+1] = DimensionDesc(dim.getBaseName(),
                                         dim.getNamesAndAliases(),
                                         dim.getStartMin(),
                                         dim.getCurrStart(), 
                                         dim.getCurrEnd(), 
                                         dim.getEndMax(),
                                         dim.getChunkInterval(), 
                                         dim.getChunkOverlap(), 
                                         TID_INT64,
                                         dim.getFlags(), 
                                         "",
                                         dim.getComment(),
                                         dim.getFuncMapOffset(),
                                         dim.getFuncMapScale());
        }
        return ArrayDesc(arrayDesc.getName(), arrayDesc.getAttributes(), dstDims);

    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAllVersions, "allversions")


} //namespace
