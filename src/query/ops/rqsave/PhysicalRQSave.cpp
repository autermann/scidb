/*
 **
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2011 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 3 of the License, or
 * (at your option) any later version.
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

#include <string.h>
#include <cmath>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "array/Array.h"
#include "smgr/io/DBLoader.h"
#include "array/DBArray.h"
#include "array/Bitmask.h"
#include "query/QueryProcessor.h"
#include "array/Compressor.h"

#include "boost/filesystem.hpp"

using namespace std;
using namespace boost;
using namespace scidb;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.rqsave"));

class PhysicalRQSave: public PhysicalOperator
{
public:
    PhysicalRQSave(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector<ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(vector<shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() >= 1);

        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        const string& dirName = ((boost::shared_ptr<OperatorParamPhysicalExpression>&) _parameters[0])->getExpression()->evaluate().getString();

        if (dirName.size() == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_PATH_IS_EMPTY);
        }

        if (!filesystem::exists(dirName))
        {
            bool created = filesystem::create_directory(dirName);
            if (!created)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY) << dirName;
            }
        }

        ostringstream chunkFileName, offsetFileName;
        ofstream chunkFile, offsetFile;
        size_t fileOffset = 0;

        chunkFileName << dirName << "/chunks";
        offsetFileName << dirName << "/offsets";

        chunkFile.open(chunkFileName.str().c_str(), ios_base::binary | ios_base::out | ios_base::trunc);
        offsetFile.open(offsetFileName.str().c_str(), ios_base::binary | ios_base::out | ios_base::trunc);

        if (chunkFile.bad())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE) << chunkFileName.str() << errno;
        }

        if (offsetFile.bad())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE) << chunkFileName.str() << errno;
        }

        shared_ptr<Array> input = inputArrays[0];
        Attributes atts = input->getArrayDesc().getAttributes();
        for (size_t i = 0; i < atts.size(); i++)
        {
            if (atts[i].getSize() == 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "variable size attributes";
            }
            if (atts[i].isNullable())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "nullable attributes";
            }
            if (atts[i].getType() == TID_BOOL)
            {
                //iterator will need to contain bitmask only and check bits,...
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "bool datatype";
            }
        }

        // determine the atts size and max chunk, how many bytes each can occupy for this array
        // uint8_t b/c if we need more than 256 bytes to convey max_attr, this is  beyond our scope
        char attBytes = (ceil(log2(atts.size())) + 7) / 8;
        // solve for the maximum possible chunk for each attr, given a full dense case
        size_t maxChunks = input->getArrayDesc().getNumberOfChunks() / atts.size();
        char chunkBytes = (ceil(log2(maxChunks)) + 7) / 8;

        offsetFile.write(&attBytes, 1);
        offsetFile.write(&chunkBytes, 1);

        for (size_t i = 0; i < atts.size(); i++)
        {
            bool isBitmask = (atts[i].getType() == TID_INDICATOR);
            shared_ptr<ConstArrayIterator> iter = input->getConstIterator(i);
            if (isBitmask)
            {
                while (!iter->end())
                {
                    ConstChunk const& chunk = iter->getChunk();
                    shared_ptr<ConstChunkIterator> citer = chunk.getConstIterator(0);
                    Coordinates firstPos = chunk.getFirstPosition(true);

                    position_t lPosition=0;
                    position_t pPosition=0;
                    RLEEmptyBitmap bm;

                    while (!citer->end())
                    {
                        if (!citer->isEmpty())
                        {
                            bm.addPositionPair(lPosition, pPosition);
                            pPosition++;
                        }

                        lPosition++;
                        ++(*citer);
                    }
                    citer.reset();

                    uint64_t chunkNo = input->getArrayDesc().getChunkNumber(chunk.getFirstPosition(false));
                    size_t bmSize = bm.packedSize();

                    //Unfortunate: need to do double copy here. Can we avoid double copy in the real thing?
                    vector<char> buf(bmSize,0);
                    bm.pack(&buf[0]);
                    chunkFile.write(&buf[0], bmSize);

                    offsetFile.write((char *) &i, attBytes);
                    offsetFile.write((char *) &chunkNo, chunkBytes);
                    offsetFile.write((char *) &fileOffset, sizeof(size_t));

                    fileOffset += bmSize;
                    ++(*iter);
                }
            }
            else
            {
                size_t elemSize = atts[i].getSize();
                Value const& dv = atts[i].getDefaultValue();

                while (!iter->end())
                {
                    ConstChunk const& chunk = iter->getChunk();

                    ValueMap vm;
                    position_t pPosition=0;

                    shared_ptr<ConstChunkIterator> citer = chunk.getConstIterator(0);
                    while (!citer->end())
                    {
                        if (!citer->isEmpty())
                        {
                            vm[pPosition] = citer->getItem();
                            pPosition++;
                        }
                        ++(*citer);
                    }

                    //again we copy two times (or three depending on how you count)
                    //not very optimal
                    RLEPayload payload(vm, vm.size(), elemSize, dv, atts[i].getType() == TID_BOOL, false);
                    size_t payloadSize = payload.packedSize();

                    vector<char> buf(payloadSize, 0);
                    payload.pack(&buf[0]);

                    uint64_t chunkNo = input->getArrayDesc().getChunkNumber(chunk.getFirstPosition(false));
                    chunkFile.write(&buf[0], payloadSize);

                    offsetFile.write((char *) &i, attBytes);
                    offsetFile.write((char *) &chunkNo, chunkBytes);
                    offsetFile.write((char *) &fileOffset, sizeof(size_t));

                    fileOffset += payloadSize;

                    ++(*iter);
                }
            }
        }
        chunkFile.close();
        offsetFile.close();

        return inputArrays[0];
    }
};
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRQSave, "rq_save", "physical_rq_save")

} //namespace
