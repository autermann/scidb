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
 * @file PhysicalRepart.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "RepartArray.h"
#include "network/NetworkManager.h"
#include "array/DelegateArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRepart: public PhysicalOperator
{
  private:
    RepartAlgorithm _algorithm;
    bool _denseOpenOnce;
    uint64_t _sequenceScanThreshold;

  public:
    PhysicalRepart(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const & schema) :
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _algorithm(static_cast<RepartAlgorithm>(
                       Config::getInstance()->
                       getOption<int>(CONFIG_REPART_ALGORITHM))),
        _denseOpenOnce(Config::getInstance()->
                       getOption<bool>(CONFIG_REPART_DENSE_OPEN_ONCE)),
        _sequenceScanThreshold(Config::getInstance()->
                               getOption<int>(CONFIG_REPART_SEQ_SCAN_THRESHOLD))
    {
    }

    virtual bool changesDistribution(
            std::vector<ArrayDesc> const& sourceSchema) const
    {
        if (sourceSchema.size() != 1) {
            throw SYSTEM_EXCEPTION(
                        SCIDB_SE_OPERATOR,
                        SCIDB_LE_UNSUPPORTED_INPUT_ARRAY)
                    << getLogicalName();
        }

        Dimensions const& source = sourceSchema[0].getDimensions();
        Dimensions const& result = _schema.getDimensions();

        for (size_t i = 0, count = source.size(); i < count; ++i)
        {
            uint32_t sourceInterval = source[i].getChunkInterval();
            uint32_t resultInterval = result[i].getChunkInterval();
            if (sourceInterval != resultInterval) {
                return true;
            }
        }
        return false;
    }

    virtual bool outputFullChunks(
            std::vector<ArrayDesc> const& sourceSchema) const
    {
        if (sourceSchema.size() != 1) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << getLogicalName();
        }

        Dimensions const& source = sourceSchema[0].getDimensions();
        Dimensions const& result = _schema.getDimensions();

        for (size_t i = 0, count = source.size(); i < count; ++i)
        {
            uint32_t sourceInterval = source[i].getChunkInterval();
            uint32_t resultInterval = result[i].getChunkInterval();
            if (sourceInterval < resultInterval) {
                return false;
            }
            if (sourceInterval % resultInterval != 0) {
                return false;
            }
            uint32_t sourceOverlap =  source[i].getChunkOverlap();
            uint32_t resultOverlap =  result[i].getChunkOverlap();
            if (sourceOverlap < resultOverlap) {
                return false;
            }
        }
        return true;
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& sourceBoundaries,
            std::vector<ArrayDesc> const& sourceSchema) const
    {
        if (sourceBoundaries.size() != 1 ||
                sourceSchema.size() != 1) {
            throw SYSTEM_EXCEPTION(
                        SCIDB_SE_OPERATOR,
                        SCIDB_LE_UNSUPPORTED_INPUT_ARRAY)
                    << getLogicalName();
        }
        return sourceBoundaries[0];
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& sourceDistribution,
            std::vector< ArrayDesc> const& sourceSchema) const
    {
        if (sourceDistribution.size() != 1 ||
                sourceSchema.size() != 1) {
            throw SYSTEM_EXCEPTION(
                        SCIDB_SE_OPERATOR,
                        SCIDB_LE_UNSUPPORTED_INPUT_ARRAY)
                    << getLogicalName();
        }
        if (changesDistribution(sourceSchema)) {
            return ArrayDistribution(psUndefined);
        } else {
            return sourceDistribution[0];
        }
    }
    /**
          * Repart is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
          * that overrides the chunkiterator method.
          */
    boost::shared_ptr<Array> execute(
            std::vector< boost::shared_ptr<Array> >& sourceArray,
            boost::shared_ptr<Query> query)
    {
        if (sourceArray.size() != 1) {
            throw SYSTEM_EXCEPTION(
                        SCIDB_SE_OPERATOR,
                        SCIDB_LE_UNSUPPORTED_INPUT_ARRAY)
                    << getLogicalName();
        }

        if (sourceArray[0]->getSupportedAccess() != Array::RANDOM)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR,
                                   SCIDB_LE_UNSUPPORTED_INPUT_ARRAY)
                    << getLogicalName();
        }

        std::vector<ArrayDesc> sourceSchema(1);
        sourceSchema[0] = sourceArray[0]->getArrayDesc();
        bool singleInstance = (false == (query->getInstancesCount() > 1));

        return boost::shared_ptr<Array>(
                    createRepartArray(_schema, sourceArray[0], query,
                                      _algorithm,
                                      _denseOpenOnce,
                                      outputFullChunks(sourceSchema),
                                      singleInstance,
                                      _tileMode,
                                      _sequenceScanThreshold));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRepart, "repart", "physicalRepart")

}  // namespace scidb
