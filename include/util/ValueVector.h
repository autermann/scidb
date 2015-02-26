/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright © 2010 - 2012 Paradigm4 Inc.
* All Rights Reserved.
*
* END_COPYRIGHT
*/

/**
 * ValueVector.h
 *
 *  Created on: Jun 18, 2012
 *      Author: dzhang
 *  This file defines utility methods for a vector of Values.
 */

#ifndef VALUEVECTOR_H_
#define VALUEVECTOR_H_

#include <vector>
#include <../src/smgr/io/DimensionIndex.h>

namespace scidb
{

/**
 * A wrapper class for comparing two vector<Value> using a single attribute.
 * @return -1 (if i1<i2); 0 (if i1==i2); 1 (if i1>i2)
 */
class CompareValueVectorsByOneValue
{
  private:
    uint32_t _attrId;
    AttributeComparator _comp;

  public:
    inline int operator()(const std::vector<Value>& i1, const std::vector<Value>& i2) {
        assert(i1.size()==i2.size());
        assert(_attrId < i1.size());

        if (_comp(i1[_attrId], i2[_attrId])) {
            return -1;
        }
        if (_comp(i2[_attrId], i1[_attrId])) {
            return 1;
        }
        return 0;
    }

    CompareValueVectorsByOneValue(uint32_t attrId, TypeId typeId): _attrId(attrId), _comp(typeId) {
    }
};

}


#endif /* VALUEVECTOR_H_ */
