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
 * LogicalStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <map>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;
using namespace boost;

namespace scidb {

class LogicalStore: public  LogicalOperator
{
public:
	LogicalStore(const string& logicalName, const std::string& alias):
	        LogicalOperator(logicalName, alias)
	{
        _properties.tile = true;
		ADD_PARAM_INPUT()
		ADD_PARAM_OUT_ARRAY_NAME()
	}

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        assert(arrayName.find('@') == std::string::npos);
        string baseName = arrayName.substr(0, arrayName.find('@'));
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(baseName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalNodeId(),
                                                                                     SystemCatalog::LockDesc::COORD,
                                                                                     SystemCatalog::LockDesc::WR));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
	{
		assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        string arrayName = ((shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc const& srcDesc = schemas[0];

        //Ensure attributes names uniqueness.
        ArrayDesc dstDesc;
        if (!SystemCatalog::getInstance()->getArrayDesc(arrayName, dstDesc, false))
        {
            Attributes outAttrs;
            map<string, uint64_t> attrsMap;
            BOOST_FOREACH(const AttributeDesc &attr, srcDesc.getAttributes())
            {
                AttributeDesc newAttr;
                if (!attrsMap.count(attr.getName()))
                {
                    attrsMap[attr.getName()] = 1;
                    newAttr = attr;
                }
                else
                {
                    while (true) { 
                        stringstream ss;
                        ss << attr.getName() << "_" << ++attrsMap[attr.getName()];
                        if (attrsMap.count(ss.str()) == 0) {
                            newAttr = AttributeDesc(attr.getId(), ss.str(), attr.getType(), attr.getFlags(),
                                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                attr.getDefaultValueExpr(), attr.getComment());
                            attrsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outAttrs.push_back(newAttr);
            }

            Dimensions outDims;
            map<string, uint64_t> dimsMap;
            BOOST_FOREACH(const DimensionDesc &dim, srcDesc.getDimensions())
            {
                DimensionDesc newDim;
                if (!dimsMap.count(dim.getBaseName()))
                {
                    dimsMap[dim.getBaseName()] = 1;
                    newDim = dim;
                }
                else
                {
                    while (true) { 
                        stringstream ss;
                        ss << dim.getBaseName() << "_" << ++dimsMap[dim.getBaseName()];
                        if (dimsMap.count(ss.str()) == 0) {
                            newDim = DimensionDesc(ss.str(), dim.getStartMin(), dim.getCurrStart(), dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval(), dim.getChunkOverlap(), dim.getType(), dim.getSourceArrayName(), dim.getComment());
                            dimsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outDims.push_back(newDim);
            }
            return ArrayDesc(arrayName, outAttrs, outDims, srcDesc.getFlags());
        }
        else
        {
            Dimensions const& srcDims = srcDesc.getDimensions();
            Dimensions const& dstDims = dstDesc.getDimensions();

            //FIXME: Need more clear message and more granular condition
            if (srcDims.size() != dstDims.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            for (size_t i = 0, n = srcDims.size(); i < n; i++)
            {
                if (!(srcDims[i].getStartMin() == dstDims[i].getStartMin()
                           && (srcDims[i].getEndMax() == dstDims[i].getEndMax() 
                               || (i == 0
                                   && srcDims[i].getEndMax() <= dstDims[i].getEndMax() 
                                   && ((srcDims[i].getLength() % srcDims[i].getChunkInterval()) == 0
                                       || srcDesc.getEmptyBitmapAttribute() != NULL)))
                           && srcDims[i].getChunkInterval() == dstDims[i].getChunkInterval()
                           && srcDims[i].getChunkOverlap() == dstDims[i].getChunkOverlap()
                           && srcDims[i].getType() == dstDims[i].getType()))
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            
            Attributes const& srcAttrs = srcDesc.getAttributes();
            Attributes const& dstAttrs = dstDesc.getAttributes();

            if (srcAttrs.size() != dstAttrs.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            for (size_t i = 0, n = srcAttrs.size(); i < n; i++) { 
                if(srcAttrs[i].getType() != dstAttrs[i].getType() || (!dstAttrs[i].isNullable() && srcAttrs[i].isNullable()))
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            return ArrayDesc(dstDesc.getId(), arrayName, dstDesc.getAttributes(), dstDesc.getDimensions(), dstDesc.getFlags());
        }
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
