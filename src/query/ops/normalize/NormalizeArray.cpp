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
 * NormalizeArray.cpp
 *
 *  Created on: Jun 30, 2010
 *      Author: Knizhnik
 */

#include "NormalizeArray.h"

namespace scidb {

using namespace boost;

NormalizeChunkIterator::NormalizeChunkIterator(DelegateChunk const* chunk, int iterationMode, double len):
        DelegateChunkIterator(chunk, iterationMode), _value(TypeLibrary::getType(TID_DOUBLE))
{
    _len = len;
}

 Value& NormalizeChunkIterator::getItem()
{
    // TODO: insert converter here
    _value.setDouble(DelegateChunkIterator::getItem().getDouble() / _len);
    return _value;
}

DelegateChunkIterator* NormalizeArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
{
    return new NormalizeChunkIterator(chunk, iterationMode, _len);
}

NormalizeArray::NormalizeArray(ArrayDesc const& schema, boost::shared_ptr<Array> inputArray, double len)
: DelegateArray(schema, inputArray, false)
{
    _len = len;
}

}
