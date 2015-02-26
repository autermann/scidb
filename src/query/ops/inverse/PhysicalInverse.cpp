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
 * PhysicalInverse.cpp
 *
 *  Created on: Mar 9, 2010
 */
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"
#include "network/NetworkManager.h"

namespace scidb
{

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
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INVERSE_ERROR1);
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

class PhysicalInverse: public PhysicalOperator
{
public:
	PhysicalInverse(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
		_schema = schema;
	}

	//This will be needed when we support inverse on multiple instances
    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psLocalInstance);
    }

	/***
	 */
	boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
            boost::shared_ptr<Query> query)
	{
        boost::shared_ptr<Array> input = inputArrays[0];
        if ( query->getInstancesCount() > 1) {
           uint64_t coordinatorID = (int64_t)query->getCoordinatorID() == -1 ? query->getInstanceID() : query->getCoordinatorID();
            input = redistribute(input, query, psLocalInstance, "", coordinatorID);
            if ( query->getInstanceID() != coordinatorID) { 
                return boost::shared_ptr<Array>(new MemArray(_schema));
            }
        }
        Dimensions const& dims = _schema.getDimensions();
        size_t length = dims[0].getLength();
        Coordinates first(2);
        Coordinates last(2);
        first[0] = dims[0].getStart();
        first[1] = dims[1].getStart();
        last[0] = dims[0].getEndMax();
        last[1] = dims[1].getEndMax();
        double* matrix = new double[length*length];
        input->extractData(0, matrix, first, last);
        inverse(matrix, length);
        shared_array<char> buf(reinterpret_cast<char*>(matrix));
		return boost::shared_ptr<Array>(new SplitArray(_schema, buf, first, last));
	}

private:
	ArrayDesc _schema;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInverse, "inverse", "PhysicalInverse")

} //namespace scidb
