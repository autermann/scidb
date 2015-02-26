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
 * @file AllVersionsArray.h
 *
 * @brief The implementation of array AllVersions
 *
 */

#ifndef ALLVERSIONS_ARRAY_H_
#define ALLVERSIONS_ARRAY_H_

#include "array/DelegateArray.h"

namespace scidb
{

using namespace boost;

class AllVersionsArray;

class AllVersionsChunkIterator : public DelegateChunkIterator
{
  public:
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);

    AllVersionsChunkIterator(DelegateChunk const* chunk, int iterationMode, VersionID version);

  private:
    VersionID currVersion;
    Coordinates outPos;
};

class AllVersionsChunk : public DelegateChunk
{
    friend class AllVersionsArray;
  public:
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    virtual void setInputChunk(ConstChunk const& inputChunk, VersionID version);

    AllVersionsChunk(DelegateArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);

  private:
    VersionID   currVersion;
    Coordinates firstPos;
    Coordinates lastPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPosWithOverlap;
};

class AllVersionsArrayIterator : public DelegateArrayIterator
{
  public:
   AllVersionsArrayIterator(AllVersionsArray const& array, AttributeID attrID,
                            boost::shared_ptr<ConstArrayIterator> inputIterator);

        virtual ConstChunk const& getChunk();

        virtual bool end();
        virtual void operator ++();
        virtual Coordinates const& getPosition();
        virtual bool setPosition(Coordinates const& pos);
        virtual void reset();
        virtual boost::shared_ptr<Query> getQuery();

  protected:
    AllVersionsArray const& array;
    shared_ptr<Array> inputVersion;
    VersionID currVersion;
    bool hasCurrent;
    Coordinates outPos;
};

class AllVersionsArray : public DelegateArray
{
    friend class AllVersionsArrayIterator;
public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;

    /**
     * Get the least restrictive access mode that the array supports.
     * Need to override this because we are a subclass of DelegateArray but we we don't set the member
     * DelegateArray::inputArray which would cause DelegateArray::getSupportedAccess() to segfault.
     * @return RANDOM
     */
    virtual Access getSupportedAccess() const
    {
        return RANDOM;
    }

    string getVersionName(VersionID version) const;

    AllVersionsArray(ArrayDesc const& desc,
                     vector<VersionDesc> const& versions,
                     boost::shared_ptr<Query>& query);

  private:
    vector<VersionDesc> versions;
    boost::weak_ptr<Query> _query;
};

}

#endif
