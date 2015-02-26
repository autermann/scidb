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
 * @file PhysicalTranspose.h
 *
 * @brief Transpose array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef PHYSICAL_TRANSPOSE_H
#define PHYSICAL_TRANSPOSE_H

#include "array/DelegateArray.h"
#include <map>

namespace scidb {

class PhysicalTranspose : public PhysicalOperator
{
public:
    PhysicalTranspose(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        ArrayDistribution inputDistro = inputDistributions[0];

        if (inputDistro == ArrayDistribution(psByRow)) {
            return ArrayDistribution(psByCol);
        }
        else if (inputDistro == ArrayDistribution(psByCol)) {
            return ArrayDistribution(psByRow);
        } else {
            //TODO:OPTAPI mapper
            return ArrayDistribution(psUndefined);
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty()) {
            return PhysicalBoundaries::createEmpty(
                        _schema.getDimensions().size());
        }

        Coordinates start = inputBoundaries[0].getStartCoords();
        size_t const nDims = start.size();
        Coordinates newStart(nDims);

        for (size_t i = 0; i < nDims; i++) {
            newStart[nDims-i-1] = start[i];
        }

        Coordinates end = inputBoundaries[0].getEndCoords();
        Coordinates newEnd(nDims);

        for (size_t i = 0; i < nDims; i++) {
            newEnd[nDims-i-1] = end[i];
        }

        return PhysicalBoundaries(newStart, newEnd);
    }
};

} /* namespace scidb */

#endif /* PHYSICAL_TRANSPOSE_H */

