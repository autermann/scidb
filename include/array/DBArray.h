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
 * DBArray.h
 *
 *  Created on: 17.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Database array implementation
 */

#ifndef DBARRAY_H_
#define DBARRAY_H_

#include <vector>
#include "array/MemArray.h"

using namespace std;
using namespace boost;

namespace scidb
{

    /**
     * Implementation of database array
     */
    class DBArray : public Array
    {
        string getRealName() const;
      public:
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        DBArray(ArrayDesc const& desc, const boost::shared_ptr<Query>& query);
        DBArray(ArrayID id, const boost::shared_ptr<Query>& query);
        DBArray(std::string const& name, const boost::shared_ptr<Query>& query);
        DBArray(const DBArray& other);

      private:
        ArrayDesc _desc;
        boost::weak_ptr<Query> _query;
    };
}

#endif
