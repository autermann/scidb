/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef ARRAY_ALLOCATION_BUFFER_H_
#define ARRAY_ALLOCATION_BUFFER_H_

/****************************************************************************/

#include "array/Array.h"                                 // For SharedBuffer

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 *  @brief      A SharedBuffer that works with operator new.
 *
 *  @details    Class AllocationBuffer offers a simple implementation of the
 *              SharedBuffer interface that is suitable for use with operator
 *              new. For example,
 *  @code
 *                  AllocationBuffer buffer;
 *
 *                  ... = new (buffer) Object(...);
 *  @endcode
 *              allocates the underlying storage for the object from memory
 *              that is owned by the SharedBuffer 'buffer'.
 *
 *  @author jbell@paradigm4.com
 */
class AllocationBuffer : public SharedBuffer, boost::noncopyable
{
 public:                   // Construction
                              AllocationBuffer():_data(0),_size(0) {}
                             ~AllocationBuffer()         {this->free();}

 public:                   // Operations
    virtual void*             getData()            const {return _data;}
    virtual size_t            getSize()            const {return _size;}
    virtual bool              pin()                const {return false;}
    virtual void              unPin()              const {}

 public:                   // Operations
    virtual void              allocate(size_t n)         {_data = new char[_size=n];}
    virtual void              free()                     {delete[]_data;_data = 0;}

 private:                  // Representation
            char*             _data;                     // The allocation
            size_t            _size;                     // Its size
};

/****************************************************************************/
}
/****************************************************************************/

inline void* operator new(size_t sz,scidb::SharedBuffer& ab)
{
    ab.allocate(sz);                                     // Allocate storage
    assert(ab.getSize() >= sz);                          // Validate override
    return ab.getData();                                 // Return allocation
}

/****************************************************************************/
#endif
/****************************************************************************/
