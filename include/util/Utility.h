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

#ifndef UTIL_UTILITY_H_
#define UTIL_UTILITY_H_

/****************************************************************************/

#include <boost/utility.hpp>

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 *  @brief      Prevents subclasses from being allocated on the heap.
 *
 *  @details    Class stackonly hides its new operators to prevent it, and any
 *              class that might derive from it, from being directly allocated
 *              on the heap. It mimics similar boost utility classes that seek
 *              to constrain the semantics of a class through inheritance, and
 *              though not entirely foolproof, nevertheless serves as a useful
 *              hint that an object is being initialized for its side effect.
 *
 *              It's useful when implementing the RIIA idiom, where it ensures
 *              that the lifetime of an object is tied to the lexical scope in
 *              which it is instantiated:
 *  @code
 *              class Lock : stackonly, boost::nocopyable
 *              {
 *                 Lock(...) ...
 *                ~Lock(...) ...
 *              }  lock(...);
 *  @endcode
 *              since without allocating an object on the heap there is no way
 *              for it to escape the current program block.
 *
 *  @see        http://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Requiring_or_Prohibiting_Heap-based_Objects
 *              for more on the underlying idiom.
 *
 *  @see        http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 *              for more on the RIIA pattern.
 *
 *  @see        http://www.boost.org/doc/libs/1_54_0/libs/utility/utility.htm
 *              for boost::noncopyable.
 *
 *  @author     jbell@paradigm4.com
 */
class stackonly
{
            void*             operator new(size_t);
            void*             operator new[](size_t);
            void              operator delete(void*);
            void              operator delete[](void*);
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
