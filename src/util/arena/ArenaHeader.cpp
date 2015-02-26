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

/****************************************************************************/

#include "ArenaHeader.h"                                 // For Header

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Return the number of array elements in the allocation.
 */
size_t Header::getElementCount() const
{
    if (!has(vectorFinalizer))                           // No count_t field?
    {
        return 1;                                        // ...a 1 is implied
    }

    if (!has(customFinalizer))                           // No finalizer_t?
    {
        return *rewind<size_t>(this);                    // ...so it's here
    }

    return *rewind<size_t>(rewind<finalizer_t>(this));   // Skip finalizer_t
}

/**
 *  Return a pointer to the function that will finalize this allocation, or 0
 *  if the allocation does not need finalizing.
 */
finalizer_t Header::getFinalizer() const
{
    if (!has(finalizer))                                 // Trivial destructor?
    {
        return 0;                                        // ...so no finalizer
    }

    if (!has(customFinalizer))                           // No finalizer_t?
    {
        return &arena::finalize<Allocated>;              // ...so ~Allocated()
    }

    return *rewind<finalizer_t>(this);                   // In front of header
}

/**
 *  Finalize at most the first 'count' elements of the allocation by applying
 *  the finalizer to each element in turn, from last to first,  thus ensuring
 *  that elements of the array are destroyed in the opposite order to that in
 *  which they were created.
 */
void Header::finalize(count_t count)
{
    if (finalizer_t f = getFinalizer())                  // Not yet finalized?
    {
        size_t  n = getElementSize();                    // ...size of element
        size_t  c = std::min(getElementCount(),count);   // ...el's to destroy
        byte_t* p = getPayload();                        // ...head of payload
        byte_t* q = p + c * n;                           // ...tail of payload

     /* Finalize each of the elements from the end of the array back toward
        its beginning, that is, in the opposite order to that in which they
        were first constructed...*/

        for (size_t i=0; i!=c; ++i)                      // ...for each element
        {
            f(q -= n);                                   // ....call finalizer
        }

     /* Clear the 'finalizer' flag to signal that the allocation is dead...*/

        _flags &= ~finalizer;                            // ...dead as a dodo
    }
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool Header::consistent() const
{
 /* A vector finalizer implies having a non-trivial array element count...*/

    if (has(vectorFinalizer))                            // Vector finalizer?
    {
        assert(getElementCount() >= 2);                  // ...proper count
    }
    else
    {
        assert(getElementCount() == 1);                  // ...trivial count
    }

    assert(has(finalizer) == (getFinalizer()!=0));       // Check finalizer()

    return true;                                         // Appears to be good
}

/****************************************************************************/
}}
/****************************************************************************/
