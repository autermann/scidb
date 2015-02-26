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
 * PhysicalFlip.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <map>
#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "query/ops/aggregates/Aggregator.h"

#include <log4cxx/logger.h>

namespace scidb {

// Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Array"));

    using namespace std;
    using namespace boost;

    #define FLIP      (1 << 31)
    #define SYNTHETIC (1 << 30)

    class PhysicalFlip: public PhysicalOperator
    {
      private:
        bool _haveAggregates;

      public:
        PhysicalFlip(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _haveAggregates(_parameters.size()>1)
        {
        }

        virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                       const std::vector< ArrayDesc> & inputSchemas) const
        {
            return PhysicalBoundaries::createFromFullSchema(_schema);
        }

        virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                        const std::vector< ArrayDesc> & inputSchemas) const
        {
            if (!_haveAggregates)
            {
                return ArrayDistribution(psUndefined);
            }
            else
            {
                return ArrayDistribution(psRoundRobin);
            }
        }

        virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
        {
            return false;
        }

        virtual bool isChunkPreserving(const std::vector< ArrayDesc> & inputSchemas) const
        {
            return _haveAggregates;
        }


        class IteratorsMap : public map< Coordinates, vector< shared_ptr<ChunkIterator> > >
        { 
          public:
            ~IteratorsMap()
            {
                for (map< Coordinates, vector< shared_ptr<ChunkIterator> > >::iterator i = begin();
                     i != end();
                     ++i)
                {
                    vector< shared_ptr<ChunkIterator> > const& v = i->second;
                    for (size_t j = 0, n = v.size(); j < n; j++) {
                        if (v[j]) {
                            v[j]->flush();
                        }
                    }
                }
            }
        };

        shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
        {
            assert(inputArrays.size() == 1);
            shared_ptr<Array> srcArray = inputArrays[0];
            ArrayDesc  const& srcDesc = srcArray->getArrayDesc();

            vector<AggregatePtr> aggregates (_schema.getAttributes().size());
            vector<AttributeID> aggregateInputs (_schema.getAttributes().size());

            if (_haveAggregates)
            {
                for(size_t i =1; i<_parameters.size(); i++)
                {
                    AttributeID inputAttId;
                    string aggOutputName;
                    AggregatePtr agg = resolveAggregate((shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                                                        srcDesc.getAttributes(),
                                                        &inputAttId,
                                                        &aggOutputName);
                    bool found = false;
                    if (inputAttId == (AttributeID) -1)
                    {
                        inputAttId = 0;
                    }

                    for (size_t j = 0; j<_schema.getAttributes().size(); j++)
                    {
                        if (_schema.getAttributes()[j].getName() == aggOutputName)
                        {
                            aggregates[j] = agg;
                            aggregateInputs[j] = inputAttId;
                            found = true;
                        }
                    }
                    assert(found);
                }
            }

            ArrayDesc dstDesc(_schema.getName(), Attributes(), _schema.getDimensions());
            for (size_t i =0; i<_schema.getAttributes().size(); i++)
            {
                Value defaultNull;
                defaultNull.setNull(0);

                if (aggregates[i].get())
                {
                    dstDesc.addAttribute( AttributeDesc(i,
                                                        _schema.getAttributes()[i].getName(),
                                                        aggregates[i]->getStateType().typeId(),
                                                        AttributeDesc::IS_NULLABLE,
                                                        0,
                                                        std::set<std::string>(),
                                                        &defaultNull,
                                                        "",
                                                        "",
                                                        aggregates[i]->getStateVarSize()));
                }
                else
                {
                    dstDesc.addAttribute(_schema.getAttributes()[i]);
                }
            }

            shared_ptr<MemArray> dstArray(new MemArray(dstDesc));
            {
                Attributes const& dstAttrs = dstDesc.getAttributes();
                Attributes const& srcAttrs = srcDesc.getAttributes();
                Dimensions const& dstDims = dstDesc.getDimensions();
                Dimensions const& srcDims = srcDesc.getDimensions();
                size_t nAttrs = dstAttrs.size();
                size_t nDims = dstDims.size();
                size_t nSrcAttrs = srcAttrs.size();
                size_t nSrcDims = srcDims.size();
                vector<size_t> attributesMapping(nAttrs);
                bool addEmptyIndicator = false;

                for (size_t i = 0; i < nAttrs; i++)
                {
                    if(aggregates[i].get())
                    {
                        attributesMapping[i] = aggregateInputs[i];
                        goto NextAttr;
                    }

                    for (size_t j = 0; j < nSrcAttrs; j++) {
                        if (srcAttrs[j].getName() == dstAttrs[i].getName()) {
                            attributesMapping[i] = j;
                            goto NextAttr;
                        }
                    }
                    for (size_t j = 0; j < nSrcDims; j++) {
                        if (srcDims[j].hasNameOrAlias(dstAttrs[i].getName())) {
                            attributesMapping[i] = j | FLIP;
                            goto NextAttr;
                        }
                    }

                    attributesMapping[i] = SYNTHETIC;
                    addEmptyIndicator = true;
                  NextAttr:;
                }
                vector<size_t> dimensionsMapping(nDims);
                for (size_t i = 0; i < nDims; i++) {
                    for (size_t j = 0; j < nSrcDims; j++) {
                        if (srcDims[j].hasNameOrAlias(dstDims[i].getBaseName())) {
                            dimensionsMapping[i] = j;
                            goto NextDim;
                        }
                    }
                    for (size_t j = 0; j < nSrcAttrs; j++) {
                        if (dstDims[i].hasNameOrAlias(srcAttrs[j].getName())) {
                            dimensionsMapping[i] = j | FLIP;
                            goto NextDim;
                        }
                    }
                    assert(false);
                  NextDim:;
                }
                size_t iterAttr = 0;
                bool coordinatesOnly = true;

                vector< shared_ptr<ConstArrayIterator> > arrayIterators(nSrcAttrs);
                vector< shared_ptr<ConstChunkIterator> > chunkIterators(nSrcAttrs);

                vector< shared_ptr<ArrayIterator> > dstArrayIterators(nAttrs);
                IteratorsMap dstChunkIteratorsMap;

                for (size_t i = 0; i < nAttrs; i++) {
                    size_t j = attributesMapping[i];
                    if (j != SYNTHETIC) {
                        if (!(j & FLIP)) {
                            if (coordinatesOnly) {
                                coordinatesOnly = false;
                                iterAttr = j;
                            }
                            arrayIterators[j] = srcArray->getConstIterator(j);
                        }
                        dstArrayIterators[i] = dstArray->getIterator(i);
                    }
                }
                for (size_t i = 0; i < nDims; i++) {
                    int j = dimensionsMapping[i];
                    if (j & FLIP) {
                        j &= ~FLIP;
                        if (coordinatesOnly) {
                            coordinatesOnly = false;
                            iterAttr = j;
                        }
                        arrayIterators[j] = srcArray->getConstIterator(j);
                    }
                }
                shared_ptr<ConstArrayIterator> srcArrayIterator =
                    coordinatesOnly ? srcArray->getConstIterator(0) : arrayIterators[iterAttr];
                shared_ptr<ConstChunkIterator> srcChunkIterator;
                Coordinates dstPos(nDims);
                Value coord(TypeLibrary::getType(TID_INT64));

                //
                // Loop through input array
                //
                int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS/*|(nSrcAttrs == 1 ? ConstChunkIterator::IGNORE_DEFAULT_VALUES : 0)*/; /* K&K: can we ever ignore default value if attribute is flipped ? */

                while (!srcArrayIterator->end())
                {
                    //
                    // Initialize source chunk iterators
                    //
                    if (coordinatesOnly) {
                        srcChunkIterator = srcArrayIterator->getChunk().getConstIterator(iterationMode);
                    } else {
                        for (size_t i = 0; i < nSrcAttrs; i++) {
                            if (arrayIterators[i])
                            {
                                chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(iterationMode);
                            }
                        }
                        srcChunkIterator = chunkIterators[iterAttr];
                    }

                    //
                    // Loop through the chunks content
                    //
                    while (!srcChunkIterator->end()) {
                        Coordinates const& srcPos = srcChunkIterator->getPosition();
                        for (size_t i = 0; i < nDims; i++) {
                            size_t j = dimensionsMapping[i];
                            dstPos[i] = (j & FLIP) ? chunkIterators[j & ~FLIP]->getItem().getInt64() : srcPos[j];
                        }
                        //
                        // Create destination chunks and initialize their iterators
                        //
                        Coordinates dstChunkPos = dstPos;
                        dstDesc.getChunkPositionFor(dstChunkPos);
                        vector< shared_ptr<ChunkIterator> >& dstChunkIterators = dstChunkIteratorsMap[dstChunkPos];
                        if (dstChunkIterators.size() == 0) {
                            dstChunkIterators.resize(nAttrs);
                            int mode = ChunkIterator::SPARSE_CHUNK;
                            if (!addEmptyIndicator) {
                                mode |= ChunkIterator::NO_EMPTY_CHECK;
                            }
                            for (size_t i = 0; i < nAttrs; i++) {
                                if (dstArrayIterators[i])
                                {
                                    dstChunkIterators[i] =
                                        shared_ptr<ChunkIterator>(dstArrayIterators[i]->newChunk(dstChunkPos).getIterator(query, mode));
                                    mode |= ChunkIterator::NO_EMPTY_CHECK;
                                }
                            }
                        }
                        for (size_t i = 0; i < nAttrs; i++) {
                            size_t j = attributesMapping[i];
                            if (j != SYNTHETIC)
                            {
                                dstChunkIterators[i]->setPosition(dstPos);
                                if (j & FLIP)
                                {
                                    coord.setInt64(srcPos[j & ~FLIP]);
                                    dstChunkIterators[i]->writeItem(coord);
                                }
                                else
                                {
                                    if (aggregates[i].get())
                                    {
                                        Value state = dstChunkIterators[i]->getItem();
                                        Value const& input = chunkIterators[j]->getItem();

                                        if ( !((aggregates[i]->ignoreNulls() && input.isNull()) ||
                                               (aggregates[i]->ignoreZeroes() && input.isZero())))
                                        {
                                            if (state.getMissingReason() == 0)
                                            {
                                                aggregates[i]->initializeState(state);
                                            }

                                            aggregates[i]->accumulate(state, input);
                                        }

                                        dstChunkIterators[i]->writeItem(state);
                                    }
                                    else
                                    {
                                        dstChunkIterators[i]->writeItem(chunkIterators[j]->getItem());
                                    }
                                }
                            }
                        }
                        //
                        // Advance chunk iterators
                        //
                        if (coordinatesOnly) {
                            ++(*srcChunkIterator);
                        } else {
                            for (size_t i = 0; i < nSrcAttrs; i++) {
                                if (chunkIterators[i]) {
                                    ++(*chunkIterators[i]);
                                }
                            }
                        }
                    }
                    //
                    // Advance array iterators
                    //
                    srcChunkIterator.reset();
                    if (coordinatesOnly) {
                        ++(*srcArrayIterator);
                    } else {
                        for (size_t i = 0; i < nSrcAttrs; i++) {
                            if (arrayIterators[i]) {
                                chunkIterators[i].reset();
                                ++(*arrayIterators[i]);
                            }
                        }
                    }
                }
            }

            if (_haveAggregates)
            {
                dstArray = redistributeAggregate(dstArray, query, aggregates);
                return boost::shared_ptr<Array>(new FinalResultArray(_schema, dstArray, aggregates));
            }

            return dstArray;
        }
    };

    DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFlip, "redimension", "physicalFlip")

}  // namespace ops
