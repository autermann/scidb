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
 * @file StackAlloc.h
 *
 * @brief Stack allocator (release all allocated memory in destructor)
 *
 */

#ifndef __STACK_ALLOC_H__
#define  __STACK_ALLOC_H__

#include <stdlib.h>
#include <assert.h>

namespace scidb
{
    const size_t STACK_ALLOC_BLOCK_SIZE = 64*1024;

    class BlockAllocator 
    {
        struct Block {
            Block* next;
            char data[STACK_ALLOC_BLOCK_SIZE];
        };
        Block* chain;
        size_t used;
        size_t count;

      public:
        BlockAllocator() {
            chain = NULL;
            used = STACK_ALLOC_BLOCK_SIZE;
            count = 1;
        }
        
        ~BlockAllocator() 
        {
            Block *curr, *next;
            for (curr = chain; curr != NULL; curr = next) {
                next = curr->next;
                delete curr;
            }
        }

        BlockAllocator* copy() 
        {
            count += 1;
            return this;
        }

        void release() 
        { 
            if (--count == 0) { 
                delete this;
            }
        }

        void* allocate(size_t size) 
        {
            assert(size <= STACK_ALLOC_BLOCK_SIZE);
            size = (size + sizeof(size) - 1) & ~(sizeof(size) - 1);
            if (size + used > STACK_ALLOC_BLOCK_SIZE) { 
                Block* block = new Block();
                block->next = chain;
                chain = block;
                used = 0;
            }
            void* p = (void*)&chain->data[used];
            used += size;
            return p;
        }
    };
        


    template<typename _Tp>
    class StackAlloc
    {
      public:
        BlockAllocator* alloc;

        typedef size_t     size_type;
        typedef ptrdiff_t  difference_type;
        typedef _Tp*       pointer;
        typedef const _Tp* const_pointer;
        typedef _Tp&       reference;
        typedef const _Tp& const_reference;
        typedef _Tp        value_type;
                        
        template<typename _Tp1>
        struct rebind
        { typedef StackAlloc<_Tp1> other; };
        
        StackAlloc() throw() { alloc = new BlockAllocator(); }
        
        StackAlloc(const StackAlloc& __a) throw() { alloc = __a.alloc->copy(); }
        
        template<typename _Tp1>
        StackAlloc(const StackAlloc<_Tp1>& __a) throw() { alloc = __a.alloc->copy(); }
        
        ~StackAlloc() throw() { 
            alloc->release();
        }

        void construct(pointer p, const _Tp& val)
        {
            new(static_cast<void*>(p)) _Tp(val);
        }

        pointer address(reference x) const {
            return static_cast<pointer>(x);
        }

        const_pointer address(const_reference x) const {
            return static_cast<const_pointer>(x);
        }

        pointer allocate(size_type size, void const* hint) {
            return hint != 0 ? static_cast<pointer>(hint) : allocate(size);
        }

        pointer allocate(size_type size) {
            return static_cast<pointer>(alloc->allocate(size*sizeof(_Tp)));
        }

        void deallocate(pointer p, size_type n) {}

        size_type max_size() const throw() {
            return STACK_ALLOC_BLOCK_SIZE;
        }

        void destroy(pointer p) {
            p->~_Tp();
        }
    };

    template <class T1, class T2>
    bool operator==(const StackAlloc<T1>& a1, const StackAlloc<T2>& a2)
        throw()
    {
        return a1.alloc == a2.alloc;
    }

    template <class T1, class T2>
    bool operator!=(const StackAlloc<T1>& a1, const StackAlloc<T2>& a2)
        throw()
    {
        return !(a1 == a2);
    }

}

#endif
