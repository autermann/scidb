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
 * PhysicalAnalyze.cpp
 *
 *  Created on: Feb 1, 2012
 *      Author: egor.pugin@gmail.com
 */

#include <log4cxx/logger.h>

#include "PhysicalAnalyze.h"
#include "DistinctCounter.h"

namespace scidb {

using namespace boost;
using namespace std;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.analyze"));

//returns unsigned 32
//error about 4%
//best one
uint32_t PhysicalAnalyze::fnv1a32(uint8_t *value, size_t size)
{
    uint32_t hash = 2166136261;
    for (size_t i = 0; i < size; i++)
    {
        hash ^= value[i];
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return hash;
}

//current hash
uint64_t PhysicalAnalyze::hash(uint8_t *value, size_t size)
{
    return fnv1a32(value, size);
}

uint64_t PhysicalAnalyze::hash(uint64_t value)
{
    return hash((uint8_t *)&value, sizeof(uint64_t));
}

PhysicalAnalyze::PhysicalAnalyze(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
{
}

boost::shared_ptr<Array> PhysicalAnalyze::execute(vector<boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
{
    map<string /*name*/, size_t /*id*/> atts_id;
    Attributes atts = inputArrays[0]->getArrayDesc().getAttributes();
    boost::shared_ptr<Array> resultArray = createTmpArray(_schema);
    const AttributeDesc *emptyIndicator = inputArrays[0]->getArrayDesc().getEmptyBitmapAttribute();

    for (size_t i = 0; i < _parameters.size(); i++)
    {
        string attName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectName();

        if (emptyIndicator && emptyIndicator->getName() == attName)
            continue;

        atts_id[attName] = 0;
    }

    size_t attributesCount = atts_id.size() == 0 ? (emptyIndicator ? atts.size() - 1 : atts.size()) : atts_id.size();

    if (!atts_id.empty())
    {
        for (size_t i = 0; i < atts.size(); i++)
        {
            const string& name = atts[i].getName();            
            if (atts_id.find(name) != atts_id.end())
            {
                atts_id[name] = atts[i].getId();
            }
        }
    }        
    else
    {
        for (size_t i = 0; i < atts.size(); i++)
        {
            if (emptyIndicator && atts[i].getId() == emptyIndicator->getId())
                continue;

            atts_id[atts[i].getName()] = atts[i].getId();
        }
    }

    LOG4CXX_DEBUG(logger, "Starting analyze");

    // main loop
    size_t index = 0;
    vector<AnalyzeData> data(attributesCount);
    for (map<string, size_t>::iterator attributeId = atts_id.begin(); attributeId != atts_id.end(); attributeId++, index++)
    {
        LOG4CXX_DEBUG(logger, "Analyzing " << (*attributeId).first << " attribute");

        data[index].attribute_name = (*attributeId).first;
        boost::shared_ptr<ConstArrayIterator> arrIt = inputArrays[0]->getConstIterator((*attributeId).second);

        TypeId typeId = atts[(*attributeId).second].getType();
        bool builtInType = isBuiltinType(typeId);

        if (builtInType && typeId != TID_STRING)
        {
            analyzeBuiltInType(&data[index], arrIt, typeId, query);
        }
        else
        {
            analyzeStringsAndUDT(&data[index], arrIt, typeId, query);
        }
    }
    // end of main loop

    if (query->getInstanceID() != 0)
    {
        return resultArray;
    }
        
    // output
    vector<boost::shared_ptr<ArrayIterator> > resultIterator(ANALYZE_ATTRIBUTES);
    vector<boost::shared_ptr<ChunkIterator> > cIter(ANALYZE_ATTRIBUTES);
    for (size_t i = 0; i < ANALYZE_ATTRIBUTES; i++)
    {            
        resultIterator[i] = resultArray->getIterator(i);
    }

    for (size_t i = 0; i < attributesCount; i++)
    {
        if (i % ANALYZE_CHUNK_SIZE == 0)
        {
            for (size_t j = 0; j < ANALYZE_ATTRIBUTES; j++)
            {
                if (cIter[j])
                    cIter[j]->flush();

                Chunk& chunk = resultIterator[j]->newChunk(Coordinates(1, i));
                chunk.setRLE(false);
                chunk.setSparse(false);
                cIter[j] = chunk.getIterator(query);
            }
        }

        Value v;
                        
        v = Value(TypeLibrary::getType(TID_STRING));
        v.setString(data[i].attribute_name.c_str());
        cIter[0]->writeItem(v);
        ++(*cIter[0]);

        v.setString(data[i].min.c_str());
        cIter[1]->writeItem(v);
        ++(*cIter[1]);

        v.setString(data[i].max.c_str());
        cIter[2]->writeItem(v);
        ++(*cIter[2]);

        v = Value(TypeLibrary::getType(TID_UINT64));            
        v.setUint64(data[i].distinct_count);
        cIter[3]->writeItem(v);
        ++(*cIter[3]);

        v.setUint64(data[i].non_null_count);
        cIter[4]->writeItem(v);
        ++(*cIter[4]);
    }

    for (size_t j = 0; j < ANALYZE_ATTRIBUTES; j++)
        cIter[j]->flush();
    // end of output

    LOG4CXX_DEBUG(logger, "Analyze is finished");

    return resultArray;
}

void PhysicalAnalyze::analyzeBuiltInType(AnalyzeData *data, boost::shared_ptr<ConstArrayIterator> arrIt, TypeId typeId, boost::shared_ptr<Query> query)
{
    boost::unordered_map<uint64_t, size_t> valueContainer;

    uint64_t memoryStep = 1.5 * (8 + sizeof(size_t));
    uint64_t maxValues = max(ANALYZE_MAX_MEMORY_PER_THREAD_BYTES / memoryStep, (uint64_t)ANALYZE_MAX_PRECISE_COUNT);

    bool useDC = false;
    DistinctCounter DC;

    Expression expr;
    expr.compile("<", false, typeId, typeId, TID_BOOL);
    ExpressionContext eContext(expr);

    //2 - double, 1 - float, 0 - other
    size_t isReal = 0;
    if (typeId == TID_DOUBLE)
    {
        isReal = 2;
    }
    else if (typeId == TID_FLOAT)
    {
        isReal = 1;
    }

    bool firstValue = false;
    Value min, max;

    while (!arrIt->end())
    {
        boost::shared_ptr<ConstChunkIterator> cIter = arrIt->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS    | 
                                                                                         ConstChunkIterator::IGNORE_EMPTY_CELLS | 
                                                                                         ConstChunkIterator::IGNORE_NULL_VALUES);

        if (!useDC)
        {
            while (!cIter->end())
            {
                Value &v = cIter->getItem();

                if (v.isNull() || (isReal == 2 && isnan(v.getDouble())) || (isReal == 1 && isnan(v.getFloat())))
                {
                    ++(*cIter);
                    continue;
                }

                if (!firstValue)
                {
                    min = max = v;
                    firstValue = true;
                }

                if (!useDC)
                {
                    uint64_t t = *(uint64_t *)v.data();
                    valueContainer[t] = hash(t);

                    //we can use this check every 1000 of values, or no
                    if (valueContainer.size() > maxValues)
                    {
                        //do conversion from hash table to DC
                        for (boost::unordered_map<uint64_t, size_t>::iterator i = valueContainer.begin(); i != valueContainer.end(); i++)
                        {
                            DC.addValue((*i).second);
                        }
                        valueContainer.clear();

                        useDC = true;
                    }
                }
                else
                {
                    DC.addValue(hash(*(uint64_t *)v.data()));
                }

                //min/max checks
                eContext[0] = v;

                eContext[1] = min;
                if (*(uint64_t *)expr.evaluate(eContext).data())
                    min = v;

                eContext[1] = max;
                if (!*(uint64_t *)expr.evaluate(eContext).data())
                    max = v;

                //non null counter
                data->non_null_count++;

                ++(*cIter);
            }
        }
        else
        {
            while (!cIter->end())
            {                    
                Value &v = cIter->getItem();

                if (v.isNull() || (isReal == 2 && isnan(v.getDouble())) || (isReal == 1 && isnan(v.getFloat())))
                {
                    ++(*cIter);
                    continue;
                }

                DC.addValue(hash(*(uint64_t *)v.data()));
                    
                //min/max checks
                eContext[0] = v;

                eContext[1] = min;
                if (*(uint64_t *)expr.evaluate(eContext).data())
                    min = v;

                eContext[1] = max;
                if (!*(uint64_t *)expr.evaluate(eContext).data())
                    max = v;

                //non null counter
                data->non_null_count++;

                ++(*cIter);
            }
        }

        ++(*arrIt);
    }

    LOG4CXX_DEBUG(logger, "Send/receive stage");

    //send/receive
    size_t nInstances = query->getInstancesCount();

    if (query->getInstanceID() == 0)
    {
        for (size_t i = 1; i < nInstances; i++)
        {                
            //receive non_null_count
            uint64_t non_null_count;
            Receive((void*)&query, i, &non_null_count, sizeof(uint64_t));
            data->non_null_count += non_null_count;

            //receive min, max
            size_t receiveMinMax;
            Receive((void*)&query, i, &receiveMinMax, sizeof(size_t));

            if (receiveMinMax == 1)
            {
                Value minValue(TypeLibrary::getType(typeId));
                Value maxValue(TypeLibrary::getType(typeId));
            
                Receive((void*)&query, i, minValue.data(), sizeof(uint64_t));
                Receive((void*)&query, i, maxValue.data(), sizeof(uint64_t));

                eContext[0] = minValue;
                eContext[1] = min;
                if (*(uint64_t *)expr.evaluate(eContext).data())
                    min = minValue;

                eContext[0] = maxValue;
                eContext[1] = max;
                if (!*(uint64_t *)expr.evaluate(eContext).data())
                    max = maxValue;
            }

            //receive DC
            size_t remoteUseDC;
            Receive((void*)&query, i, &remoteUseDC, sizeof(size_t));

            size_t size;
            Receive((void*)&query, i, &size, sizeof(size_t));

            if (size != 0)
            {
                if (!remoteUseDC)
                {
                    boost::scoped_array<uint64_t> bufVal(new uint64_t[size]);
                    boost::scoped_array<size_t> bufHash(new size_t[size]);

                    Receive((void*)&query, i, bufVal.get(), size * sizeof(uint64_t));
                    Receive((void*)&query, i, bufHash.get(), size * sizeof(size_t));

                    if (!useDC)
                    {
                        for (size_t i = 0; i < size; i++)
                        {
                            valueContainer[bufVal[i]] = bufHash[i];
                        }

                        if (valueContainer.size() > maxValues)
                        {
                            //do conversion from hash table to DC
                            for (boost::unordered_map<uint64_t, size_t>::iterator i = valueContainer.begin(); i != valueContainer.end(); i++)
                            {
                                DC.addValue((*i).second);
                            }
                            valueContainer.clear();

                            useDC = true;
                        }
                    }
                    else
                    {
                        for (size_t i = 0; i < size; i++)
                        {
                            DC.addValue(bufHash[i]);
                        }
                    }
                }
                else
                {
                    boost::scoped_array<uint8_t> dc(new uint8_t[size]);

                    Receive((void*)&query, i, dc.get(), size * sizeof(uint8_t));

                    DC.mergeDC(dc.get(), size);

                    if (!useDC)
                    {
                        //do conversion from hash table to DC
                        for (boost::unordered_map<uint64_t, size_t>::iterator i = valueContainer.begin(); i != valueContainer.end(); i++)
                        {
                            DC.addValue((*i).second);
                        }
                        valueContainer.clear();

                        useDC = true;
                    }
                }//if (!remoteUseDC)
            }//if (size != 0)
        }//for (size_t i = 1; i < nInstances; i++)
    }//if (query->getInstanceID() == 0)
    //send
    else
    {
        //send non_null_count
        Send((void*)&query, 0, &data->non_null_count, sizeof(uint64_t));

        //send min, max
        size_t sendMinMax = 1;

        if (min.getMissingReason() == 0)
        {
            sendMinMax = 0;
        }

        Send((void*)&query, 0, &sendMinMax, sizeof(size_t));

        if (sendMinMax == 1)
        {
            Send((void*)&query, 0, (uint64_t *)min.data(), sizeof(uint64_t));
            Send((void*)&query, 0, (uint64_t *)max.data(), sizeof(uint64_t));
        }

        //send DC
        //send type
        size_t type = useDC;
        Send((void*)&query, 0, &type, sizeof(size_t));

        if (!useDC)
        {
            size_t size = valueContainer.size();
            size_t index = 0;
            boost::scoped_array<uint64_t> bufVal(new uint64_t[size]);
            boost::scoped_array<size_t> bufHash(new size_t[size]);

            for (boost::unordered_map<uint64_t, size_t>::iterator i = valueContainer.begin(); i != valueContainer.end(); i++, index++)
            {
                bufVal[index] = (*i).first;
                bufHash[index] = (*i).second;
            }
            valueContainer.clear();

            Send((void*)&query, 0, &size, sizeof(size_t));

            if (size != 0)
            {
                Send((void*)&query, 0, bufVal.get(), size * sizeof(uint64_t));
                Send((void*)&query, 0, bufHash.get(), size * sizeof(size_t));
            }
        }
        else
        {
            size_t size;
            boost::shared_array<uint8_t>& dc = DC.getDC(&size);

            Send((void*)&query, 0, &size, sizeof(size_t));

            if (size != 0)
            {
                Send((void*)&query, 0, dc.get(), size * sizeof(uint8_t));
            }
        }
    }
    //end of send/receive

    data->min = ValueToString(typeId, min);
    data->max = ValueToString(typeId, max);

    data->distinct_count = useDC ? DC.getCount() : valueContainer.size();
}

void PhysicalAnalyze::analyzeStringsAndUDT(AnalyzeData *data, boost::shared_ptr<ConstArrayIterator> arrIt, TypeId typeId, boost::shared_ptr<Query> query)
{
    boost::unordered_map<vector<uint8_t>, size_t> valueContainerForStrings;

    size_t memory = 0;

    bool useDC = false;
    DistinctCounter DC;

    Expression expr;
    expr.compile("<", false, typeId, typeId, TID_BOOL);
    ExpressionContext eContext(expr);

    Value min, max;

    if (!arrIt->end())
    {
        max = min = arrIt->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS    | 
                                                       ConstChunkIterator::IGNORE_EMPTY_CELLS | 
                                                       ConstChunkIterator::IGNORE_NULL_VALUES)->getItem();
    }

    while (!arrIt->end())
    {
        boost::shared_ptr<ConstChunkIterator> cIter = arrIt->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS    | 
                                                                                         ConstChunkIterator::IGNORE_EMPTY_CELLS | 
                                                                                         ConstChunkIterator::IGNORE_NULL_VALUES);

        if (!useDC)
        {
            while (!cIter->end())
            {
                Value &v = cIter->getItem();

                if (v.isNull())
                {
                    ++(*cIter);
                    continue;
                }

                if (!useDC)
                {
                    size_t size = v.size();
                    vector<uint8_t> s(size, 0);
                    memcpy(&s[0], v.data(), size);
                    valueContainerForStrings[s] = hash((uint8_t *)v.data(), size);
                    memory += size;

                    //we can use this check every 1000 of values, or no
                    if (memory > ANALYZE_MAX_MEMORY_PER_THREAD_BYTES || valueContainerForStrings.size() > ANALYZE_MAX_PRECISE_COUNT)
                    {
                        //do conversion from hash table to DC
                        for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = valueContainerForStrings.begin(); i != valueContainerForStrings.end(); i++)
                        {
                            DC.addValue(hash((uint8_t *)(*i).first.data(), (*i).first.size()));
                        }
                        valueContainerForStrings.clear();

                        useDC = true;
                    }
                }
                else
                {
                    DC.addValue(hash((uint8_t *)v.data(), v.size()));
                }

                //min/max checks
                eContext[0] = v;

                eContext[1] = min;
                if (*(uint64_t *)expr.evaluate(eContext).data())
                    min = v;

                eContext[1] = max;
                if (!*(uint64_t *)expr.evaluate(eContext).data())
                    max = v;

                //non null counter
                data->non_null_count++;

                ++(*cIter);
            }
        }
        else
        {
            while (!cIter->end())
            {                    
                Value &v = cIter->getItem();

                if (v.isNull())
                {
                    ++(*cIter);
                    continue;
                }

                DC.addValue(hash((uint8_t *)v.data(), v.size()));
                    
                //min/max checks
                eContext[0] = v;

                eContext[1] = min;
                if (*(uint64_t *)expr.evaluate(eContext).data())
                    min = v;

                eContext[1] = max;
                if (!*(uint64_t *)expr.evaluate(eContext).data())
                    max = v;

                //non null counter
                data->non_null_count++;

                ++(*cIter);
            }
        }

        ++(*arrIt);
    }

    LOG4CXX_DEBUG(logger, "Send/receive stage");

    //send/receive
    size_t nInstances = query->getInstancesCount();

    if (query->getInstanceID() == 0)
    {
        for (size_t i = 1; i < nInstances; i++)
        {                
            //receive non_null_count
            uint64_t non_null_count;
            Receive((void*)&query, i, &non_null_count, sizeof(uint64_t));
            data->non_null_count += non_null_count;

            //receive min, max
            size_t receiveMinMax;
            Receive((void*)&query, i, &receiveMinMax, sizeof(size_t));

            if (receiveMinMax == 1)
            {
                size_t sz;

                Receive((void*)&query, i, &sz, sizeof(size_t));
                Value minValue(sz);
                if (sz != 0)
                {
                    Receive((void*)&query, i, minValue.data(), sz);

                    eContext[0] = minValue;
                    eContext[1] = min;
                    if (*(uint64_t *)expr.evaluate(eContext).data())
                        min = minValue;
                }

                Receive((void*)&query, i, &sz, sizeof(size_t));
                Value maxValue(sz);
                if (sz != 0)
                {
                    Receive((void*)&query, i, maxValue.data(), sz);

                    eContext[0] = maxValue;
                    eContext[1] = max;
                    if (!*(uint64_t *)expr.evaluate(eContext).data())
                        max = maxValue;
                }
            }

            //receive DC
            size_t remoteUseDC;
            Receive((void*)&query, i, &remoteUseDC, sizeof(size_t));

            size_t size;
            Receive((void*)&query, i, &size, sizeof(size_t));

            if (size != 0)
            {
                if (!remoteUseDC)
                {
                    boost::unordered_map<vector<uint8_t>, size_t> tempContainer;

                    for (size_t j = 0; j < size; j++)
                    {
                        size_t sz;
                        Receive((void*)&query, i, &sz, sizeof(size_t));

                        if (sz != 0)
                        {
                            vector<uint8_t> s(sz, 0);
                            Receive((void*)&query, i, &s[0], sz);

                            tempContainer[s] = hash((uint8_t *)s.data(), sz);
                        }
                    }

                    if (!useDC)
                    {
                        for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = tempContainer.begin(); i != tempContainer.end(); i++)
                        {
                            valueContainerForStrings[(*i).first] = (*i).second;
                            memory += (*i).first.size();
                        }

                        if (memory > ANALYZE_MAX_MEMORY_PER_THREAD_BYTES || valueContainerForStrings.size() > ANALYZE_MAX_PRECISE_COUNT)
                        {
                            //do conversion from hash table to DC
                            for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = valueContainerForStrings.begin(); i != valueContainerForStrings.end(); i++)
                            {
                                DC.addValue((*i).second);
                            }
                            valueContainerForStrings.clear();

                            useDC = true;
                        }
                    }
                    else
                    {
                        for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = tempContainer.begin(); i != tempContainer.end(); i++)
                        {
                            DC.addValue((*i).second);
                        }
                        tempContainer.clear();
                    }
                }
                else
                {
                    boost::scoped_array<uint8_t> dc(new uint8_t[size]);

                    Receive((void*)&query, i, dc.get(), size * sizeof(uint8_t));

                    DC.mergeDC(dc.get(), size);

                    if (!useDC)
                    {
                        //do conversion from hash table to DC
                        for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = valueContainerForStrings.begin(); i != valueContainerForStrings.end(); i++)
                        {
                            DC.addValue((*i).second);
                        }
                        valueContainerForStrings.clear();

                        useDC = true;
                    }
                }//if (!remoteUseDC)
            }//if (size != 0)
        }//for (size_t i = 1; i < nInstances; i++)
    }//if (query->getInstanceID() == 0)
    //send
    else
    {
        //send non_null_count
        Send((void*)&query, 0, &data->non_null_count, sizeof(uint64_t));

        //send min, max
        size_t sendMinMax = 1;

        if (min.getMissingReason() == 0)
        {
            sendMinMax = 0;
        }

        Send((void*)&query, 0, &sendMinMax, sizeof(size_t));

        if (sendMinMax == 1)
        {
            size_t sz;
            
            sz = min.size();
            Send((void*)&query, 0, &sz, sizeof(size_t));
            if (sz != 0)
            {
                Send((void*)&query, 0, (uint8_t *)min.data(), min.size());
            }

            sz = max.size();
            Send((void*)&query, 0, &sz, sizeof(size_t));
            if (sz != 0)
            {
                Send((void*)&query, 0, (uint8_t *)max.data(), max.size());
            }
        }

        //send DC
        //send type
        size_t type = useDC;
        Send((void*)&query, 0, &type, sizeof(size_t));

        if (!useDC)
        {
            size_t size = valueContainerForStrings.size();
            Send((void*)&query, 0, &size, sizeof(size_t));

            if (size != 0)
            {
                for (boost::unordered_map<vector<uint8_t>, size_t>::iterator i = valueContainerForStrings.begin(); i != valueContainerForStrings.end(); i++)
                {
                    size_t sz = (*i).first.size();
                    Send((void*)&query, 0, &sz, sizeof(size_t));
                    if (sz != 0)
                    {
                        Send((void*)&query, 0, (*i).first.data(), (*i).first.size());
                    }
                }
                valueContainerForStrings.clear();
            }
        }
        else
        {
            size_t size;
            boost::shared_array<uint8_t>& dc = DC.getDC(&size);

            Send((void*)&query, 0, &size, sizeof(size_t));
            if (size != 0)
            {
                Send((void*)&query, 0, dc.get(), size * sizeof(uint8_t));
            }
        }
    }
    //end of send/receive

    //conversions to string
    if (typeId != TID_STRING)
    {
        FunctionPointer p = FunctionLibrary::getInstance()->findConverter(typeId, TID_STRING);

        if (p)
        {
            boost::shared_ptr<const Value *> args = boost::shared_ptr<const Value *>(new const Value*);
            Value res;
            
            *args = &min;
            p(args.get(), &res, 0);
            data->min = res.getString();
            
            *args = &max;
            p(args.get(), &res, 0);
            data->max = res.getString();
        }
        else
        {
            data->min = ValueToString(typeId, min);
            data->max = ValueToString(typeId, max);
        }
    }
    else
    {
        data->min = min.getString();
        data->max = max.getString();
    }

    data->distinct_count = useDC ? DC.getCount() : valueContainerForStrings.size();
}

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAnalyze, "analyze", "physicalAnalyze")

}  // namespace scidb
