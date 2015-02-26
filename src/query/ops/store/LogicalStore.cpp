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
#include <smgr/io/Storage.h>

using namespace std;
using namespace boost;

namespace scidb {

inline string stripVersion(string const& s)
{
    size_t v = s.find('@');
    return (v != string::npos) ? (s.substr(0, v) + s.substr(s.find(':'))) : s;
}

/**
 * @brief The operator: store().
 *
 * @par Synopsis:
 *   store( srcArray, outputArray )
 *
 * @par Summary:
 *   Stores an array to the database. Each execution of store() causes a new version of the array to be created.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - outputArray: an existing array in the database, with the same schema as srcArray.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
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
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(arrayName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalInstanceId(),
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

        //query->exclusiveLock(arrayName);

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
                    newDim = DimensionDesc(dim.getBaseName(), 
                                           dim.getStartMin(), 
                                           dim.getCurrStart(), 
                                           dim.getCurrEnd(), 
                                           dim.getEndMax(), 
                                           dim.getChunkInterval(), 
                                           dim.getChunkOverlap(), 
                                           dim.getType(), 
                                           dim.getFlags(), 
                                           dim.getMappingArrayName(), 
                                           dim.getComment(),
                                           dim.getFuncMapOffset(),
                                           dim.getFuncMapScale());
                }
                else
                {
                    while (true) { 
                        stringstream ss;
                        ss << dim.getBaseName() << "_" << ++dimsMap[dim.getBaseName()];
                        if (dimsMap.count(ss.str()) == 0) {
                            newDim = DimensionDesc(ss.str(), 
                                                   dim.getStartMin(), 
                                                   dim.getCurrStart(), 
                                                   dim.getCurrEnd(), 
                                                   dim.getEndMax(), 
                                                   dim.getChunkInterval(), 
                                                   dim.getChunkOverlap(), 
                                                   dim.getType(), 
                                                   dim.getFlags(), 
                                                   dim.getMappingArrayName(), 
                                                   dim.getComment(),
                                                   dim.getFuncMapOffset(),
                                                   dim.getFuncMapScale());
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
                               || (srcDims[i].getEndMax() < dstDims[i].getEndMax() 
                                   && ((srcDims[i].getLength() % srcDims[i].getChunkInterval()) == 0
                                       || srcDesc.getEmptyBitmapAttribute() != NULL)))
                           && srcDims[i].getChunkInterval() == dstDims[i].getChunkInterval()
                           && srcDims[i].getChunkOverlap() == dstDims[i].getChunkOverlap()
                           && srcDims[i].getType() == dstDims[i].getType()))
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
                }
                if (srcDims[i].getType() != TID_INT64 && stripVersion(srcDims[i].getMappingArrayName()) != stripVersion(dstDims[i].getMappingArrayName()) && !dstDims[i].getMappingArrayName().empty() && srcDims[i].getMappingArrayName().empty())
                {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_STORE_ERROR1);
                }
            }

            Attributes const& srcAttrs = srcDesc.getAttributes(true);
            Attributes const& dstAttrs = dstDesc.getAttributes(true);

            if (srcAttrs.size() != dstAttrs.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            for (size_t i = 0, n = srcAttrs.size(); i < n; i++) { 
                if(srcAttrs[i].getType() != dstAttrs[i].getType() || (!dstAttrs[i].isNullable() && srcAttrs[i].isNullable()))
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            Dimensions newDims(dstDims.size());
            for (size_t i = 0; i < dstDims.size(); i++) { 
                DimensionDesc const& dim = dstDims[i];
                newDims[i] = DimensionDesc(dim.getBaseName(),
                                           dim.getNamesAndAliases(),
                                           dim.getStartMin(), dim.getCurrStart(),
                                           dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval(),
                                           dim.getChunkOverlap(), dim.getType(), dim.getFlags(),
                                           srcDims[i].getMappingArrayName(),
                                           dim.getComment(),
                                           dim.getFuncMapOffset(),
                                           dim.getFuncMapScale());
            }
            return ArrayDesc(dstDesc.getId(), dstDesc.getUAId(), dstDesc.getVersionId(), arrayName, dstDesc.getAttributes(), newDims, dstDesc.getFlags());
        }
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
