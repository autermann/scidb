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
 * InverseArray.cpp
 *
 *  Created on: Mar 9, 2010
 */
#include "query/ops/inverse/InverseArray.h"
#include "log4cxx/logger.h"
#include <math.h>

namespace scidb
{
// Logger for this operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.ops.inverse"));

// code to do the inverse
int inverse(double* a, int size)  // in-place variant
{
// a - non-symmetric matrix a[size,size]

        double *b;   b=new double[size];
        double *tin; tin=new double[size];
        double *matr; matr=new double[size*size];
        int* ind;  ind=new int[size];

        double eps(1.e-10);
        int  i(0),k(0),l(0),ii(0),j(0);
        double x(0.0),y(0.0),s(0.0);

        for(i=0;i<size;i++)
        {
                tin[i]=0.; ind[i]=0;
                 for(j=0;j<size;j++) matr[i*size+j]=0.;
        }       //      io

//
// check if original matrix has zero row  => terminate
//
        for(i=0;i<size;i++)
        {
                s=0.0;
                for(j=0; j<size; j++) s=s+a[i*size+j]*a[i*size+j];
                y=s;
                if(y==0.0)
                {
                        delete []b; delete []tin; delete []ind; delete []matr;
                        if (0.0 == y)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INVERSE_ERROR1);
                        return 1;
                }       //  y==0
                else  tin[i]=1./sqrt(y);
        }               //      i

//
// decompose matrix a=LU
// where L is lower triangular matrix
//       U is upper triangular matrix with diagonal equals to 1
//  factors L and U are stored in a

    for(k=0; k<size; k++)
    {
        l=k;  x=0.;
        for(i=k; i<size; i++)
        {
                s=-a[i*size+k];
                for(j=0; j<=(k-1); j++) s=s+a[i*size+j]*a[j*size+k];
                y=s; a[i*size+k]=-y;  y=fabs(y*tin[i]);
                if(y>x){ x=y; l=i;}
        }       // for(i=k; i<size; i++)

        if(l!=k)
        {
                   for(j=0; j<size; j++)
                   {y=a[k*size+j]; a[k*size+j]=a[l*size+j]; a[l*size+j]=y;}
                   ind[l]=ind[k];
        }               //       if(l!=k)
       ind[k]=l;

       if(x<8*eps)
       {
            delete []b; delete []tin; delete []ind; delete []matr;
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INVERSE_ERROR2);
       }

       x=-1./a[k*size+k];
       for(j=k+1; j<size; j++)
           {
                s=-a[k*size+j];
                for(i=0; i<=(k-1); i++) s=s+a[k*size+i]*a[i*size+j];
//              y=s; matr[k*size+j]=x*y;
                y=s; a[k*size+j]=x*y;
           }            //      for(j=k+1; j<size; j++)
        }               //              k

//
// stage II :  solve systems Ly=b and Ux=y  size times
//  with right-hand side b equals to zero everywhere except k-position (=1)
//

   for(k=0;k<size; k++)
   {
           for(i=0; i<size; i++) b[i]=0.;
           b[k]=1.;
// Ly=B
       for(i=0; i<size; i++)
           {
                   if(ind[i]!=i){x=b[i]; ii=ind[i]; b[i]=b[ii]; b[ii]=x;}
                   s=b[i];
                   for(j=0; j<=(i-1); j++) s=s+a[i*size+j]*b[j];
                   x=s;  b[i]=-x/a[i*size+i];
           }            //              i

//  Ux=y
           for(i=(size-1); i>=0; i--)
           {
                   s=b[i];
           for(j=(i+1); j<size; j++) s=s+a[i*size+j]*b[j];
                   x=s; b[i]=-x;
           }
           for(i=0; i<size; i++) matr[i*size+k]=b[i];
   }            //  k

//===========================================================

   for(i=0; i<size; i++) for(k=0; k<size; k++) a[i*size+k]=matr[i*size+k];


   delete []b; delete []tin; delete []ind; delete []matr;
   return 0;

}                       //              end of inverse


    //
    // InverseArrayIterator methods
    //
    void InverseArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Dimensions const& dims = array.desc.getDimensions();
        chunkInitialized = false;
        for (int i = currPos.size(); --i >= 0;) {
            currPos[i] += dims[i].getChunkInterval();
            if (currPos[i] > dims[i].getEndMax()) {
                currPos[i] = dims[i].getStart();
            } else {
                return;
            }
        }
        hasCurrent = false;
    }

    bool InverseArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& InverseArrayIterator::getPosition()
    {
        return currPos;
    }

    bool InverseArrayIterator::setPosition(Coordinates const& pos)
    {
        Dimensions const& dims = array.desc.getDimensions();
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            if (pos[i] < dims[i].getStart() || pos[i] > dims[i].getEndMax()) {
                return hasCurrent = false;
            }
        }
        currPos = pos;
        chunkInitialized = false;
        return hasCurrent = true;
    }

    void InverseArrayIterator::reset()
    {
        //LOG4CXX_DEBUG(logger, "InverseArrayIterator::reset()" );
        Dimensions const& dims = array.desc.getDimensions();
        hasCurrent = true;
        chunkInitialized = false;
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            currPos[i] = dims[i].getStart();
            hasCurrent &= dims[i].getLength() != 0;
        }
    }

    /***
     * Return a chunk from the pre-computed result of the inverse
     */
    ConstChunk const& InverseArrayIterator::getChunk()
    {
        LOG4CXX_DEBUG(logger, "Inverse: fetch chunk " << currPos[0] << ", " << currPos[1] );
        if (!chunkInitialized) { 
            // Initialize the chunk
            Address addr(array.desc.getId(), 0, currPos);
            chunk.setSparse(true);
            chunk.initialize(&array, &array.desc, addr, 0);
            boost::shared_ptr<Query> emptyQuery;
            boost::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(emptyQuery,
                                                                         ChunkIterator::IGNORE_OVERLAPS);
            
            // The position coordinates can point to any element in the chunk, so we need to obtain the
            // first position in the chunk
            
            int ci = currPos[0] - (currPos[0] % iChunkLen);
            int cj = currPos[1] - (currPos[1] % jChunkLen);
            
            Coordinates coords;
            coords.push_back(0);
            coords.push_back(0);
            
            // Fill in the data for this chunk from the pre-computed matrix
            int i=0,j=0;
            while( i<iChunkLen && j<jChunkLen )
            {
                coords[0] = currPos[0] + i;
                coords[1] = currPos[1] + j;
                chunkIt->setPosition(coords);
                Value v(TypeLibrary::getType(TID_DOUBLE));
                v.setDouble(matrix[(ci+i)*jLength + cj+j]);
                chunkIt->writeItem(v);
                
                i++;
                if (i == iChunkLen)
                {
                    i=0;
                    j++;
                }
            }
            
            chunkIt->flush();
            chunkInitialized = true;
        }
        return chunk;
    }

    InverseArrayIterator::InverseArrayIterator(InverseArray const& arr, AttributeID attrID)
    : array(arr),
     currPos(arr.desc.getDimensions().size()),
      chunk()
    {
        reset();
        iChunkLen = arr.desc.getDimensions()[1].getChunkInterval();
        jChunkLen = arr.desc.getDimensions()[0].getChunkInterval();
        iLength = arr.desc.getDimensions()[1].getLength();
        jLength = arr.desc.getDimensions()[0].getLength();
        LOG4CXX_DEBUG(logger, "InverseArray: iCL " << iChunkLen << ", jCL " << jChunkLen << ", iL " << iLength << ", jl " << jLength);

        // Compute the inverse of the matrix
        computeInverse();
    }

    InverseArrayIterator::~InverseArrayIterator()
    {
        delete matrix;
    }

    // Computes the inverse of the matrix
    void InverseArrayIterator::computeInverse()
    {
        // Allocate the matrix
        matrix = new double[iLength * jLength];

        // Fill in the matrix with input data
        boost::shared_ptr<ConstArrayIterator> arrayIt = array.inputArray->getConstIterator(0);
        for(int ci=0,cj=0; !arrayIt->end(); ci+=iChunkLen)
        {
            if (ci >= iLength)
            {
                ci = 0;
                cj += jChunkLen;
            }

            boost::shared_ptr<ConstChunkIterator> chunkIt = arrayIt->getChunk().getConstIterator(ChunkIterator::IGNORE_OVERLAPS);

            for (int i=0,j=0; !chunkIt->end(); i++)
            {
                if (i >= iChunkLen)
                {
                    i=0;
                    j++;
                }
                // TODO: insert converter to double
                double currValue = chunkIt->getItem().getDouble();
                matrix[ (cj+j)*iLength + ci+i] = currValue;

                ++(*chunkIt);
            }
            ++(*arrayIt);
        }

        // At this point matrix holds the data for the input array
        //LOG4CXX_DEBUG(logger, "Testing matrix: m[1,2] = " << matrix[1*iLength + 2] );
        inverse(matrix, iLength);
    }

    //
    // Inverse array methods
    //

    ArrayDesc const& InverseArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ConstArrayIterator> InverseArray::getConstIterator(AttributeID attr) const
    {
        return boost::shared_ptr<ConstArrayIterator>(new InverseArrayIterator(*this, attr));
    }

    InverseArray::InverseArray(ArrayDesc aDesc, boost::shared_ptr<Array> const& aInputArray)
    : desc(aDesc),
      inputArray(aInputArray)
    {

    }

} //namespace scidb
