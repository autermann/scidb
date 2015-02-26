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
 * LookupArray.cpp
 *
 *  Created on: Jun 26, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/lookup/LookupArray.h"


using namespace std;
using namespace boost;

namespace scidb {

    //
    // Lookup chunk iterator methods
    //
    bool LookupChunkIterator::mapPosition()
    {
        if (inputIterator->end()) { 
            hasCurrent = false;
        } else { 
            size_t nDims = templateChunkIterators.size(); 
            Coordinates pos(nDims);
            for (size_t i = 0; i < nDims; i++) {
                if (!_converters[i])
                    pos[i] = templateChunkIterators[i]->getItem().getInt64();
                else {
                    Value val(TypeLibrary::getType(TID_INT64));
                    const Value* v = &templateChunkIterators[i]->getItem();
                    _converters[i](&v, &val, NULL);
                    pos[i] = val.getInt64();
                }
            }
            if (sourceArrayIterator->setPosition(pos)) { 
                sourceChunkIterator = sourceArrayIterator->getChunk().getConstIterator(iterationMode);
                hasCurrent = sourceChunkIterator->setPosition(pos);
            } else { 
                hasCurrent = false;
            }
        }
        return hasCurrent;
    }
            
    void LookupChunkIterator::reset()
    {
        for (size_t i = 0, n = templateChunkIterators.size(); i < n; i++) { 
            templateChunkIterators[i]->reset();
        }
        mapPosition();       
    }


    bool LookupChunkIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = templateChunkIterators.size(); i < n; i++) { 
            if (!templateChunkIterators[i]->setPosition(pos)) { 
                return false;
            }
        }
        return mapPosition();
    }

    Value& LookupChunkIterator::getItem()
    { 
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        return sourceChunkIterator->getItem();
    }

    void LookupChunkIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        for (size_t i = 0, n = templateChunkIterators.size(); i < n; i++) {
            ++(*templateChunkIterators[i]);
        }
        mapPosition();
    }

    bool LookupChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool LookupChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        return sourceChunkIterator->isEmpty();
    }

    LookupChunkIterator::LookupChunkIterator(LookupArrayIterator const& arrayIterator, DelegateChunk const* chunk, int mode) 
    : DelegateChunkIterator(chunk, mode),
      templateChunkIterators(chunk->getArrayDesc().getDimensions().size()),
      _converters(chunk->getArrayDesc().getDimensions().size()),
      sourceArrayIterator(arrayIterator.sourceIterator),
      iterationMode(mode)
    {
        templateChunkIterators[0] = inputIterator;
        for (size_t i = 1, n = templateChunkIterators.size(); i < n; i++) { 
            templateChunkIterators[i] = arrayIterator.templateIterators[i]->getChunk().getConstIterator(mode);
        }

        for (size_t i = 0, n = templateChunkIterators.size(); i < n; i++) {
            const TypeId attType = templateChunkIterators[i]->getChunk().getArrayDesc().getAttributes()[i].getType();
            _converters[i] = attType == TID_INT64 ? NULL :
                FunctionLibrary::getInstance()->findConverter(attType, TID_INT64);
        }

        mapPosition();
    }        

    // 
    // Lookup array iterator methods
    //

    bool LookupArrayIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = templateIterators.size(); i < n; i++) { 
            if (!templateIterators[i]->setPosition(pos)) { 
                return false;
            }
        }
        return true;
    }

    void LookupArrayIterator::reset()
    {
        for (size_t i = 0, n = templateIterators.size(); i < n; i++) { 
            templateIterators[i]->reset(); 
        }
    }

    void LookupArrayIterator::operator ++()
    {
        for (size_t i = 0, n = templateIterators.size(); i < n; i++) { 
            ++(*templateIterators[i]); 
        }
    }

    LookupArrayIterator::LookupArrayIterator(LookupArray const& array, AttributeID attrID)
    : DelegateArrayIterator(array, attrID, array.templateArray->getConstIterator(0)),
      templateIterators(array.getArrayDesc().getDimensions().size()),
      sourceIterator(array.sourceArray->getConstIterator(attrID))
    {
        templateIterators[0] = inputIterator;
        for (size_t i = 1, n = templateIterators.size(); i < n; i++) { 
            templateIterators[i] =  array.templateArray->getConstIterator(i);
        }
    }

    
    //
    // Lookup array methods
    //
    
    DelegateChunkIterator* LookupArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new LookupChunkIterator((LookupArrayIterator const&)chunk->getArrayIterator(), chunk, iterationMode);
    }

    DelegateArrayIterator* LookupArray::createArrayIterator(AttributeID attrID) const
    {
        return new LookupArrayIterator(*this, attrID);
    }

    LookupArray::LookupArray(ArrayDesc const& desc, boost::shared_ptr<Array> templArr, boost::shared_ptr<Array> srcArr)
    : DelegateArray(desc, templArr, false),
      templateArray(templArr),
      sourceArray(srcArr)
    {
    }
}
