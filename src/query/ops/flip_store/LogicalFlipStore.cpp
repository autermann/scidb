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
 * LogicalFlipStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <map>
#include <math.h>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include <smgr/io/Storage.h>

using namespace std;
using namespace boost;

namespace scidb {

const size_t DEFAULT_CHUNK_ELEMS = 1024*1024;

class LogicalFlipStore: public  LogicalOperator
{
public:
	LogicalFlipStore(const string& logicalName, const std::string& alias):
	        LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_OUT_ARRAY_NAME()
		ADD_PARAM_VARIES()
	}
	
	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		//For internal using: cleanup temporary stored flipped arrays when joining by attributes.		  
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

		res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_AGGREGATE_CALL());

		return res;
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

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        string arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc const& srcDesc = schemas[0];

        //query->exclusiveLock(arrayName);

        //Compile a desc of all possible attributes (aggregate calls first) and source dimensions
        ArrayDesc aggregationDesc (srcDesc.getName(), Attributes(), srcDesc.getDimensions());
        vector<string> aggregatedNames;

        //add aggregate calls first
        for (size_t i = 1; i < _parameters.size(); i++)
        {
            assert(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL);
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall>&) _parameters[i], srcDesc, aggregationDesc);
            aggregatedNames.push_back(aggregationDesc.getAttributes()[aggregationDesc.getAttributes().size()-1].getName());
        }

        //add other attributes
        BOOST_FOREACH(const AttributeDesc &srcAttr, srcDesc.getAttributes())
        {
            //if there's an attribute with same name as an aggregate call - skip the attribute
            bool found = false;
            BOOST_FOREACH(const AttributeDesc &aggAttr, aggregationDesc.getAttributes())
            {
                if( aggAttr.getName() == srcAttr.getName())
                {
                    found = true;
                }
            }

            if (!found)
            {
                aggregationDesc.addAttribute(AttributeDesc( aggregationDesc.getAttributes().size(),
                                                            srcAttr.getName(),
                                                            srcAttr.getType(),
                                                            srcAttr.getFlags(),
                                                            srcAttr.getDefaultCompressionMethod(),
                                                            srcAttr.getAliases(),
                                                            &srcAttr.getDefaultValue(),
                                                            srcAttr.getDefaultValueExpr(),
                                                            srcAttr.getComment(),
                                                            srcAttr.getVarSize()));
            }
        }

        ArrayDesc dstDesc;
        if (!SystemCatalog::getInstance()->getArrayDesc(arrayName, dstDesc, false))
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;

            Dimensions outDims;
            Attributes outAttrs;
            AttributeID outAttrId = 0;
            size_t nDims = 0;

            BOOST_FOREACH(const AttributeDesc& inAttr, aggregationDesc.getAttributes())
            {
                if (inAttr.getType() == TID_INDICATOR)
                    continue;

                bool isAggAttr = false;
                for (size_t i = 0; i< aggregatedNames.size(); i++)
                {
                    if(inAttr.getName() == aggregatedNames[i])
                    {
                        isAggAttr = true;
                        break;
                    }
                }

                if (!isAggAttr)
                {
                    nDims += 1;
                }
            }
            
            size_t defaultChunkSize = nDims != 0 ? (size_t)pow(DEFAULT_CHUNK_ELEMS, 1.0/nDims) : 0;

            BOOST_FOREACH(const AttributeDesc& inAttr, aggregationDesc.getAttributes())
            {
                if (inAttr.getType() == TID_INDICATOR)
                    continue;

                bool isAggAttr = false;

                for (size_t i = 0; i< aggregatedNames.size(); i++)
                {
                    if(inAttr.getName() == aggregatedNames[i])
                    {
                        isAggAttr = true;
                        break;
                    }
                }

                if (isAggAttr)
                {
                    outAttrs.push_back(AttributeDesc(outAttrId++,
                                                     inAttr.getName(),
                                                     inAttr.getType(),
                                                     inAttr.getFlags(),
                                                     inAttr.getDefaultCompressionMethod(),
                                                     inAttr.getAliases(),
                                                     &inAttr.getDefaultValue(),
                                                     inAttr.getDefaultValueExpr(),
                                                     inAttr.getComment(),
                                                     inAttr.getVarSize()));
                }
                else
                {
                    DimensionDesc dim;
                    if (inAttr.getType() == TID_INT64)
                    {
                        dim = DimensionDesc(
                                inAttr.getName(),
                                MIN_COORDINATE,
                                MAX_COORDINATE,
                                MIN_COORDINATE,
                                MAX_COORDINATE,
                                defaultChunkSize,
                                0,  
                                inAttr.getType()
                                );
                    }
                    else
                    {
                        dim = DimensionDesc(
                                inAttr.getName(),
                                0,
                                0,
                                MIN_COORDINATE,
                                MAX_COORDINATE,
                                defaultChunkSize,
                                0,
                                inAttr.getType()
                                );
                    }
                    dim.addAlias(schemas[0].getName());
                    BOOST_FOREACH(const string& attrAlias, inAttr.getAliases())
                    {
                        dim.addAlias(attrAlias);
                    }
                    outDims.push_back(dim);
                }
            }

            BOOST_FOREACH(const DimensionDesc& inDim, aggregationDesc.getDimensions())
            {
                outAttrs.push_back(
                    AttributeDesc(
                            outAttrId++,
                            inDim.getBaseName(),
                            inDim.getType(),
                            0,
                            0)
                );
            }
            outAttrs.push_back(AttributeDesc(
                    outAttrId, "empty_indicator", TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));

            if (!outDims.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR1);

            dstDesc = ArrayDesc(arrayName, outAttrs, outDims, 0);
        }

        // Let's allow to flip to non-emptyable arrays
        //if (!dstDesc.getEmptyBitmapAttribute())
        //    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR2);
 
        Attributes const& dstAttrs = dstDesc.getAttributes();
        Attributes const& srcAttrs = aggregationDesc.getAttributes();
        Dimensions dstDims = dstDesc.getDimensions();
        Dimensions const& srcDims = aggregationDesc.getDimensions();
        size_t nAttrs = dstAttrs.size();
        size_t nDims = dstDims.size();
        size_t nSrcAttrs = srcAttrs.size();
        size_t nSrcDims = srcDims.size();

        for (size_t i = 0; i < nAttrs; i++)
        {
            for (size_t j = 0; j < nSrcAttrs; j++)
            {
                if (srcAttrs[j].getName() == dstAttrs[i].getName())
                {
                    if (srcAttrs[j].getType() != dstAttrs[i].getType())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
                            << srcAttrs[j].getName() << srcAttrs[j].getType() << dstAttrs[i].getType();
                    }
                    if (!dstAttrs[i].isNullable() && srcAttrs[j].isNullable())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
                            << srcAttrs[j].getName();
                    }

                    goto NextAttr;
                }
            }
            for (size_t j = 0; j < nSrcDims; j++)
            {
                if (srcDims[j].hasNameOrAlias(dstAttrs[i].getName()))
                {
                    if (srcDims[j].getType() != dstAttrs[i].getType())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_DIMENSION_TYPE)
                            << srcDims[j].getBaseName() << srcDims[j].getType() << dstAttrs[i].getType();
                    }
                    if (dstAttrs[i].getFlags() != 0)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_FLAGS)
                            << dstAttrs[i].getName();
                    }

                    goto NextAttr;
                }
            }

            if (dstAttrs[i].isEmptyIndicator() == false)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE)
                    << dstAttrs[i].getName();
            }

            NextAttr:;
        }
            
        size_t nNewDims = 0;

        for (size_t i = 0; i < nDims; i++)
        {
            if (dstDims[i].getChunkOverlap() > dstDims[i].getChunkInterval()/2)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR3);

            for (size_t j = 0; j < nSrcDims; j++)
            {
                if (srcDims[j].hasNameOrAlias(dstDims[i].getBaseName()))
                {
                    if (dstDims[i].getStart() != srcDims[j].getStart())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR6);
                    }
                    if (dstDims[i].getType() != srcDims[j].getType()) {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR4) << srcDims[j].getBaseName();
                    }
                    goto NextDim;
                }
            }
            for (size_t j = 0; j < nSrcAttrs; j++)
            {
                if (dstDims[i].hasNameOrAlias(srcAttrs[j].getName()))
                {
                    for (size_t k = 0; k< aggregatedNames.size(); k++)
                    {
                        if(srcAttrs[j].getName() == aggregatedNames[k])
                        {
                            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_AGGREGATE_RESULT_CANT_BE_TRANSFORMED_TO_DIMENSION)
                                << srcAttrs[j].getName();
                        }
                    }

                    if ((srcAttrs[j].getType() != dstDims[i].getType() && dstDims[i].getType() != TID_INT64)
                            || 0 != srcAttrs[j].getFlags())
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR5) << srcAttrs[j].getName();

                    dstDims[i] = DimensionDesc(dstDims[i].getBaseName(), dstDims[i].getNamesAndAliases(),
                                               dstDims[i].getStartMin(), 
                                               MAX_COORDINATE,
                                               MIN_COORDINATE,
                                               dstDims[i].getEndMax(), 
                                               dstDims[i].getChunkInterval(),
                                               dstDims[i].getChunkOverlap(), 
                                               srcAttrs[j].getType(),
                                               dstDims[i].getFlags(),
                                               dstDesc.createMappingArrayName(i, 0));
                    goto NextDim;
                }
            }
            if (nNewDims++ != 0 || !aggregatedNames.empty() || dstDims[i].getType() != TID_INT64) { 
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_DIMENSION)
                    << dstDims[i].getBaseName();
            }
            NextDim:;
        }
        return ArrayDesc(dstDesc.getId(), arrayName, dstAttrs, dstDims, dstDesc.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalFlipStore, "redimension_store")

}  // namespace scidb
