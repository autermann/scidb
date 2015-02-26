#include "RedimensionCommon.h"

namespace scidb
{

using namespace std;
using namespace boost;

void RedimensionCommon::setupMappings(ArrayDesc const& srcArrayDesc,
                                       vector<AggregatePtr> & aggregates,
                                       vector<size_t>& attrMapping,
                                       vector<size_t>& dimMapping,
                                       Attributes const& destAttrs,
                                       Dimensions const& destDims)
{
    assert(aggregates.size() == attrMapping.size());
    assert(_schema.getAttributes(true).size() == aggregates.size());
    assert(_schema.getAttributes().size() == aggregates.size()+1);

    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true);
    Dimensions const& srcDims = srcArrayDesc.getDimensions();

    for(size_t i =1; i<_parameters.size(); i++)
    {
        assert(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL);
        {
            AttributeID inputAttId;
            string aggOutputName;
            AggregatePtr agg = resolveAggregate((shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                    srcArrayDesc.getAttributes(),
                    &inputAttId,
                    &aggOutputName);

            bool found = false;
            if (inputAttId == (AttributeID) -1)
            {
                inputAttId = 0;
            }

            for (size_t j = 0; j<_schema.getAttributes(true).size(); j++)
            {
                if (_schema.getAttributes()[j].getName() == aggOutputName)
                {
                    aggregates[j] = agg;
                    attrMapping[j] = inputAttId;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR6) << aggOutputName;
            }
        }
    }

    for (size_t i = 0; i < destAttrs.size(); i++)
    {
        if (aggregates[i].get())
        {//already populated
            continue;
        }

        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (srcAttrs[j].getName() == destAttrs[i].getName()) {
                attrMapping[i] = j;
                goto NextAttr;
            }
        }
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destAttrs[i].getName())) {
                attrMapping[i] = turnOn(j, FLIP);
                goto NextAttr;
            }
        }
        assert(false); // A dest attribute either comes from a src dimension or a src attribute. Can't reach here.

        NextAttr:;
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destDims[i].getBaseName())) {
                dimMapping[i] = j;
                goto NextDim;
            }
        }
        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (destDims[i].hasNameAndAlias(srcAttrs[j].getName())) {
                dimMapping[i] = turnOn(j, FLIP);
                goto NextDim;
            }
        }
        dimMapping[i] = SYNTHETIC;
        NextDim:;
    }
}

shared_ptr<Array> RedimensionCommon::redimensionArray(shared_ptr<Array> const& srcArray,
                                                      vector<size_t> const& attrMapping,
                                                      vector<size_t> const& dimMapping,
                                                      vector<AggregatePtr> const& aggregates,
                                                      shared_ptr<Query> const& query,
                                                      vector< shared_ptr<AttributeMultiMap> > const& coordinateMultiIndices,
                                                      vector< shared_ptr<AttributeMap> > const& coordinateIndices,
                                                      ElapsedMilliSeconds& timing,
                                                      bool redistributionRequired)
{
    // def of the meta data
    ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true); // true = exclude the empty tag
    Attributes const& destAttrs = _schema.getAttributes(true);
    Dimensions const& destDims = _schema.getDimensions();

    // Does the dest array have a synthetic dimension?
    bool hasSynthetic = false;
    size_t dimSynthetic = 0;
    Coordinate dimStartSynthetic = MIN_COORDINATE;
    Coordinate dimEndSynthetic = MAX_COORDINATE;

    for (size_t i=0; i<dimMapping.size(); ++i) {
        if (dimMapping[i] == SYNTHETIC) {
            hasSynthetic = true;
            dimSynthetic = i;
            dimStartSynthetic = destDims[i].getStart();
            dimEndSynthetic = dimStartSynthetic + destDims[i].getChunkInterval() - 1;
            SCIDB_ASSERT(dimEndSynthetic>=dimStartSynthetic);
            break;
        }
    }

    // Does the dest array have any aggregate?
    bool hasAggregate = false;
    for (size_t i=0; i<aggregates.size(); ++i) {
        if (aggregates[i]) {
            hasAggregate = true;
            break;
        }
    }

    // Does the dest array have any overlap?
    bool hasOverlap = false;
    for (size_t i = 0; i < destDims.size(); i++) {
        if (destDims[i].getChunkOverlap() != 0) {
            hasOverlap = true;
            break;
        }
    }

    // Create a RowCollection.
    // Each row corresponds to a chunk in the destination array.

    // The schema is adapted from destArrayDesc, with the following differences:
    //    (a) An aggregate field's type is replaced with the source field type, but still uses the name of the dest attribute.
    //        The motivation is that multiple dest aggregate attribute may come from the same source attribute,
    //        in which case storing under the source attribute name would cause a conflict.
    //    (b) An additional attribute is appended at the end, called 'tmpRowCollectionPosition', that stores the location of the item in the dest array.
    //
    // The data is derived from the inputarray as follows.
    //    (a) They are "redimensioned".
    //    (b) Each record is stored as a distinct record in the RowCollection. For an aggregate field, no aggregation is performed;
    //        For a synthetic dimension, just use dimStartSynthetic.
    //
    // Local aggregation will be performed at a later step, when generating the MemArray called 'beforeRedistribute'.
    // Global aggregation will be performed at the redistributeAggregate() step.
    //
    Attributes attrsRowCollection;
    for (size_t i=0; i<destAttrs.size(); ++i) {
        // For aggregate field, store the source data but under the name of the dest attribute.
        // The motivation is that multiple dest aggregate attribute may come from the same source attribute,
        // in which case storing under the source attribute name would cause conflict.
        //
        // An optimization is possible in this special case, to only store the source attribute once.
        // But some unintuitive bookkeeping would be needed.
        // We decide to skip the optimization at least for now.
        if (aggregates[i]) {
            AttributeDesc const& srcAttrForAggr = srcAttrs[ attrMapping[i] ];
            attrsRowCollection.push_back(AttributeDesc(i, destAttrs[i].getName(), srcAttrForAggr.getType(),
                    srcAttrForAggr.getFlags(), srcAttrForAggr.getDefaultCompressionMethod()));
        } else {
            attrsRowCollection.push_back(destAttrs[i]);
        }
    }
    attrsRowCollection.push_back(AttributeDesc(destAttrs.size(), "tmpRowCollectionPosition", TID_INT64, 0, 0));
    RowCollection<Coordinates> rowCollection(query, "", attrsRowCollection);

    //
    // Iterate through the input array, generate the output data, and append to the RowCollection.
    // Note: For an aggregate field, its source value (in the input array) is used.
    // Note: The synthetic dimension is not handled here. That is, multiple records, that will be differentiated along the synthetic dimension,
    //       are all appended to the RowCollection with the same 'position'.
    //
    size_t iterAttr = 0;    // one of the attributes from the input array that needs to be iterated

    vector< shared_ptr<ConstArrayIterator> > srcArrayIterators(srcAttrs.size());
    vector< shared_ptr<ConstChunkIterator> > srcChunkIterators(srcAttrs.size());

    for (size_t i = 0; i < destAttrs.size(); i++) {
        size_t j = attrMapping[i];

        if (!isFlipped(j)) {
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(j);
        }
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        size_t j = dimMapping[i];
        if (isFlipped(j)) {
            j = turnOff(j, FLIP);
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(j);
        }
    }
    if (!srcArrayIterators[iterAttr]) {
        // If no src attribute needs to be scanned, open one anyways.
        assert(iterAttr == 0);
        srcArrayIterators[0] = srcArray->getConstIterator(0);
    }

    ArrayCoordinatesMapper arrayCoordinatesMapper(destDims);

    while (!srcArrayIterators[iterAttr]->end())
    {
        // Initialize src chunk iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                srcChunkIterators[i] = srcArrayIterators[i]->getChunk().getConstIterator();
            }
        }

        Coordinates chunkPos;

        // Loop through the chunks content
        while (!srcChunkIterators[iterAttr]->end()) {
            Coordinates const& srcPos = srcChunkIterators[iterAttr]->getPosition();
            Coordinates destPos(destDims.size());
            vector<Value> valuesInRowCollection(destAttrs.size()+1);

            // Get the destPos for this item -- for the SYNTHETIC dim, use the same value (dimStartSynthetic) for all.
            for (size_t i = 0; i < destDims.size(); i++) {
                size_t j = dimMapping[i];
                if (isFlipped(j)) {
                    Value const& value = srcChunkIterators[turnOff(j,FLIP)]->getItem();
                    if (value.isNull()) {
                        // a dimension is NULL. Just skip this item.
                        goto ToNextItem;
                    }
                    if (destDims[i].getType() == TID_INT64) {
                        destPos[i] = value.getInt64();
                    } else {
                        if (coordinateIndices[i]) {
                            destPos[i] = coordinateIndices[i]->get(value);
                        } else {
                            destPos[i] = coordinateMultiIndices[i]->get(value);
                        }
                    }
                } else if (j == SYNTHETIC) {
                    destPos[i] = dimStartSynthetic;
                } else {
                    destPos[i] = srcPos[j];
                }

                // sanity check
                if (destPos[i]<destDims[i].getStart() || destPos[i]>destDims[i].getEndMax()) {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(destPos);
                }
            }

            chunkPos = destPos;
            _schema.getChunkPositionFor(chunkPos);

            // Build data (except the last field, i.e. position) to be written to the RowCollection.
            for (size_t i = 0; i < destAttrs.size(); i++) {
                size_t j = attrMapping[i];
                if ( isFlipped(j) ) { // if flipped from a dim and ...
                    if (destAttrs[i].getType() == TID_INT64) { // ... if this is an INT64 dim
                        valuesInRowCollection[i].setInt64( srcPos[turnOff(j, FLIP)] );
                    } else { // ... if this is an NID
                        valuesInRowCollection[i] = srcArrayDesc.getOriginalCoordinate(turnOff(j, FLIP), srcPos[turnOff(j, FLIP)], query);
                    }
                } else { // from an attribute
                    valuesInRowCollection[i] = srcChunkIterators[j]->getItem();
                }
            }

            // Set the last field of the data, and append to the RowCollection.
            if (hasOverlap) {
                OverlappingChunksIterator allChunks(destDims, destPos, chunkPos);
                while (!allChunks.end()) {
                    Coordinates const& overlappingChunkPos = allChunks.getPosition();
                    position_t pos = arrayCoordinatesMapper.coord2pos(overlappingChunkPos, destPos);
                    valuesInRowCollection[destAttrs.size()].setInt64(pos);
                    size_t resultRowId = UNKNOWN_ROW_ID;
                    rowCollection.appendItem(resultRowId, overlappingChunkPos, valuesInRowCollection);

                    // Must increment after overlappingChunkPos is no longer needed, because the increment will modify overlappingChunkPos.
                    ++allChunks;
                }
            } else {
                position_t pos = arrayCoordinatesMapper.coord2pos(chunkPos, destPos);
                valuesInRowCollection[destAttrs.size()].setInt64(pos);
                size_t resultRowId = UNKNOWN_ROW_ID;
                rowCollection.appendItem(resultRowId, chunkPos, valuesInRowCollection);
            }

            // Advance chunk iterators
            ToNextItem:

            for (size_t i = 0; i < srcAttrs.size(); i++) {
                if (srcChunkIterators[i]) {
                    ++(*srcChunkIterators[i]);
                }
            }
        }

        // Advance array iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                ++(*srcArrayIterators[i]);
            }
        }
    }

    rowCollection.switchMode(RowCollectionModeRead);
    timing.logTiming(logger, "[RedimStore] inputArray --> RowCollection");

    // Create a MemArray call 'beforeRedistribution'.
    //
    // The schema is adapted from destArrayDesc as follows:
    //    (a) For an aggregate field, the type is the 'State' of the aggregate, rather than the destination field type.
    //
    // The data is computed as follows:
    //    (a) For an aggregate field, the aggregate state, among all records with the same positin, is stored.
    //    (b) If hasSynthetic, each record with the same position get assigned a distinct value in the synthetic dimension, effectively
    //        assigning a distinct position to every record.
    //    (c) If !hasAggregate and !hasSynthetic, for duplicates, only one record is kept.
    //
    // Also, the MemArray has the empty tag, regardless to what the input array has.
    //
    Attributes attrsBeforeRedistribution;

    if (hasAggregate) {
        for (size_t i=0; i<destAttrs.size(); ++i) {
            if (aggregates[i]) {
                attrsBeforeRedistribution.push_back(AttributeDesc(i, destAttrs[i].getName(), aggregates[i]->getStateType().typeId(),
                        destAttrs[i].getFlags(), destAttrs[i].getDefaultCompressionMethod()));
            } else {
                attrsBeforeRedistribution.push_back(destAttrs[i]);
            }
        }
    } else {
        attrsBeforeRedistribution = destAttrs;
    }

    shared_ptr<MemArray> beforeRedistribution = make_shared<MemArray>(
            ArrayDesc(_schema.getName(), addEmptyTagAttribute(attrsBeforeRedistribution), _schema.getDimensions()));

    // Write data from the RowCollection to the MemArray
    vector<shared_ptr<ArrayIterator> > arrayItersBeforeRedistribution(attrsBeforeRedistribution.size());
    vector<shared_ptr<ChunkIterator> > chunkItersBeforeRedistribution(attrsBeforeRedistribution.size());
    for (size_t i=0; i<destAttrs.size(); ++i) {
        arrayItersBeforeRedistribution[i] = beforeRedistribution->getIterator(i);
    }

    CompareValueVectorsByOneValue compareValueVectorsFunc(destAttrs.size(), TID_INT64); // compare two items based on position in chunk

    RowCollection<Coordinates>::GroupToRowId const& groupToRowId = rowCollection.getGroupToRowId();
    for (RowCollection<Coordinates>::GroupToRowId::const_iterator groupToRowIdIter = groupToRowId.begin();
            groupToRowIdIter != groupToRowId.end(); ++groupToRowIdIter)
    {
        Coordinates const& chunkPos = groupToRowIdIter->first;
        size_t rowId = groupToRowIdIter->second;

        // Get the row.
        vector<vector<Value> > items;
        rowCollection.getWholeRow(rowId, items, false); // false - no separateNull
        assert(items.size()>0);
        Coordinates lows(chunkPos.size()), intervals(chunkPos.size());
        arrayCoordinatesMapper.chunkPos2LowsAndIntervals(chunkPos, lows, intervals);

        // Sort based on position in the chunk (again, all records in the same 'cell' were assigned the same value in the synthetic dim).
        iqsort(&items[0], items.size(), compareValueVectorsFunc);

        // If there is a synthetic dimension, and if there are duplicates, modify the values
        // (so that the duplicates get distinct coordinates in the synthetic dimension).
        bool needToResort = false;
        if (hasSynthetic) {
            Coordinate offset = 1; // To be added to the synthetic dimension of the current record.
            position_t prevPosition = items[0][destAttrs.size()].getInt64(); // The position to compare with.
            Coordinates coordinates(destDims.size());

            for (size_t i=1; i<items.size(); ++i) {
                position_t currPosition = items[i][destAttrs.size()].getInt64();
                if (prevPosition == currPosition) {
                    // found a duplicate ==> recalculate the position, with an increased coordinate in the synthetic dim.
                    arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, coordinates);
                    coordinates[dimSynthetic] += offset;
                    position_t newPos = arrayCoordinatesMapper.coord2posWithLowsAndIntervals(lows, intervals, coordinates);
                    items[i][destAttrs.size()].setInt64(newPos);

                    // Increment offset for the next duplicate, and mark needtoResort.
                    ++offset;
                    needToResort = true;
                } else {
                    prevPosition = currPosition;
                    offset = 1;
                }
            }
        }

        // Resort, if some position has been changed.
        if (needToResort) {
            iqsort(&items[0], items.size(), compareValueVectorsFunc);
        }

        // Create new chunks and get the iterators.
        // The first non-empty-tag attribute does NOT use NO_EMPTY_CHECK (so as to help take care of the empty tag); Others do.
        //
        int iterMode = ConstChunkIterator::SEQUENTIAL_WRITE;

        for (size_t i=0; i<destAttrs.size(); ++i) {
            Chunk& chunk = arrayItersBeforeRedistribution[i]->newChunk(chunkPos);
            chunkItersBeforeRedistribution[i] = chunk.getIterator(query, iterMode);
            iterMode |= ConstChunkIterator::NO_EMPTY_CHECK;
        }

        { // begin of scope -- needed to flush the chunks even in case of errors.
            BOOST_SCOPE_EXIT((&chunkItersBeforeRedistribution)(&destAttrs)) {
                // Flush the chunks.
                for (size_t i=0; i<destAttrs.size(); ++i) {
                    chunkItersBeforeRedistribution[i]->flush();
                    chunkItersBeforeRedistribution[i].reset();
                }
            } BOOST_SCOPE_EXIT_END

            // Scan through the items, aggregate (if apply), and write to the MemArray.
            //
            // When seeing the first item with a new position, the attribute values in the item are populated into the destItem as follows.
            //  - For a scalar field, the value is copied.
            //  - For an aggregate field, the value is initialized and accumulated.
            //
            // When seeing subsequent items with the same position, the attribute values in the item are populated as follows.
            //  - For a scalar field, the value is ignored (just select the first item).
            //  - For an aggregate field, the value is accumulated.
            //
            StateVector stateVector(aggregates, 1);  // 1 = one element to ignore at the end of every items[i]; i.e. to ignore the 'position' field.

            position_t prevPosition = 0; // The position to compare with.
            if (items.size()>0) {
                prevPosition = items[0][destAttrs.size()].getInt64();
                stateVector.init();
                stateVector.accumulate(items[0]);
            }
            Coordinates coordinates(destDims.size()); // a temporary variable into which a position is converted

            for (size_t i=1; i<items.size(); ++i) {
                position_t currPosition = items[i][destAttrs.size()].getInt64();

                if (currPosition == prevPosition) {
                    stateVector.accumulate(items[i]);
                } else {
                    // Output the previous state vector.
                    arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, coordinates);
                    vector<Value> const& destItem = stateVector.get();
                    for (size_t a=0; a<destAttrs.size(); ++a) {
                        bool rc = chunkItersBeforeRedistribution[a]->setPosition(coordinates);
                        if (!rc) {
                            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(coordinates);
                        }
                        chunkItersBeforeRedistribution[a]->writeItem(destItem[a]);
                    }

                    // record the new prevPosition
                    prevPosition = currPosition;

                    // Init and accumulate with the current item.
                    stateVector.init();
                    stateVector.accumulate(items[i]);
                }
            }

            // Write the last state vector.
            if (items.size()>0) {
                arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, coordinates);
                vector<Value> const& destItem = stateVector.get();
                for (size_t a=0; a<destAttrs.size(); ++a) {
                    bool rc = chunkItersBeforeRedistribution[a]->setPosition(coordinates);
                    if (!rc) {
                        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(coordinates);
                    }
                    chunkItersBeforeRedistribution[a]->writeItem(destItem[a]);
                }
            }
        } // end of scope
    }
    for (size_t i=0; i<destAttrs.size(); ++i) {
        arrayItersBeforeRedistribution[i].reset();
    }
    timing.logTiming(logger, "[RedimStore] RowCollection --> beforeRedistribution");

    if( !hasAggregate && !redistributionRequired)
    {
        return beforeRedistribution;
    }

    // Redistribute the MemArray using redistributeAggregate.
    // Note: redistributeAggregate() needs the 'aggregates' parameter to include empty tag.
    shared_ptr<RedimInfo> redimInfo = make_shared<RedimInfo>(hasSynthetic, dimSynthetic, destDims[dimSynthetic]);
    vector<AggregatePtr> aggregatesWithET(aggregates.size()+1);
    for (size_t i=0; i<aggregates.size(); ++i) {
        aggregatesWithET[i] = aggregates[i];
    }

    aggregatesWithET[aggregates.size()] = AggregatePtr();
    shared_ptr<MemArray> afterRedistribution = redistributeAggregate(beforeRedistribution, query, aggregatesWithET, redimInfo);
    beforeRedistribution.reset();

    timing.logTiming(logger, "[RedimStore] redistributeAggregate");
    LOG4CXX_DEBUG(logger, "[RedimStore] begin afterRedistribution --> withAggregates");

    // Scan through afterRedistribution to write the aggregate result to withAggregates (only if there are aggregate fields).
    //
    shared_ptr<MemArray> withAggregates;
    if (! hasAggregate) {
        withAggregates = afterRedistribution;
    } else {
        withAggregates = make_shared<MemArray>(
                ArrayDesc(_schema.getName(), _schema.getAttributes(), _schema.getDimensions()));

        vector<shared_ptr<ConstArrayIterator> > memArrayIters(destAttrs.size());
        vector<shared_ptr<ConstChunkIterator> > memChunkIters(destAttrs.size());
        vector<shared_ptr<ArrayIterator> > withAggregatesArrayIters(destAttrs.size());
        vector<shared_ptr<ChunkIterator> > withAggregatesChunkIters(destAttrs.size());
        set<Coordinates, CoordinatesLess> newChunks;

        for (size_t i=0; i<destAttrs.size(); ++i) {
            memArrayIters[i] = afterRedistribution->getConstIterator(i);
            withAggregatesArrayIters[i] = withAggregates->getIterator(i);
        }

        size_t chunkID = 0;
        size_t reportATimingAfterHowManyChunks = 10; // report a timing after how many chunks?

        while (!memArrayIters[0]->end()) {
            Coordinates chunkPos = memArrayIters[0]->getPosition();

            int destMode = ConstChunkIterator::SEQUENTIAL_WRITE;
            for (size_t i=0; i<destAttrs.size(); ++i) {
                // src chunk
                ConstChunk const& srcChunk = memArrayIters[i]->getChunk();
                shared_ptr<ConstChunkIterator> src = srcChunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
                src->setVectorMode(false);

                // dest chunk
                boost::shared_ptr<ChunkIterator> dst;
                Chunk& destChunk = withAggregatesArrayIters[i]->newChunk(chunkPos, srcChunk.getCompressionMethod());
                destChunk.setSparse(false);
                dst = destChunk.getIterator(query, destMode);
                destMode |= ConstChunkIterator::NO_EMPTY_CHECK; // every attribute, except the first attribute, has NO_EMPTY_CHECK.
                dst->setVectorMode(false);

                // copy
                size_t count = 0;
                while (!src->end()) {
                    ++ count;
                    Coordinates const& destPos = src->getPosition();

                    bool rc = dst->setPosition(destPos);
                    SCIDB_ASSERT(rc);

                    if (aggregates[i]) {
                        Value result;
                        aggregates[i]->finalResult(result, src->getItem());
                        dst->writeItem(result);
                    } else {
                        dst->writeItem(src->getItem());
                    }
                    ++(*src);
                }
                src.reset();

                if (!hasOverlap) { // the count should not include overlapped items; just leave as 0.
                    destChunk.setCount(count);
                }
                dst->flush();
                dst.reset();

                // timing
                if (logger->isDebugEnabled()) {
                    ++ chunkID;
                    if (chunkID%reportATimingAfterHowManyChunks ==0) {
                        char buf[100];
                        sprintf(buf, "[RedimStore] reading %lu chunks", chunkID);
                        timing.logTiming(logger, buf, false);
                        if (chunkID==100) {
                            reportATimingAfterHowManyChunks  = 100;
                            LOG4CXX_DEBUG(logger, "[RedimStore] Now reporting a number after 100 chunks.");
                        } else if (chunkID==1000) {
                            reportATimingAfterHowManyChunks = 1000;
                            LOG4CXX_DEBUG(logger, "[RedimStore] Now reporting a number after 1000 chunks.");
                        }
                    }
                }
            }

            for (size_t i=0; i<destAttrs.size(); ++i) {
                ++(*memArrayIters[i]);
            }
        }
        for (size_t i=0; i<destAttrs.size(); ++i) {
            memArrayIters[i].reset();
            withAggregatesArrayIters[i].reset();
        }
        // timing
        LOG4CXX_DEBUG(logger, "[RedimStore] total of " << chunkID << " chunks.");
        timing.logTiming(logger, "[RedimStore] afterRedistribution --> withAggregates");
    }
    return withAggregates;
}

}
