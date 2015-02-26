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
 * LogicalBuildSparse.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: build_sparse().
 *
 * @par Synopsis:
 *   build_sparse( srcArray | schema, expression, expressionIsNonEmpty )
 *
 * @par Summary:
 *   Produces a sparse array and assigns values to its non-empty cells. The schema must have a single attribute.
 *
 * @par Input:
 *   - schemaArray | schema: an array or a schema, from which attrs and dims will be used by the output array.
 *   - expression: the expression which is used to compute values for the non-empty cells.
 *   - expressionIsNonEmpty: the expression which is used to compute whether a cell is not empty.
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  2,      7
 *     <br> 2011,  3,      6
 *     <br> 2012,  1,      5
 *     <br> 2012,  2,      9
 *     <br> 2012,  3,      8
 *   - build_sparse(A, 0, item!=2) <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  1,      0
 *     <br> 2011,  3,      0
 *     <br> 2012,  1,      0
 *     <br> 2012,  3,      0
 *
 * @par Errors:
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_OP_BUILD_SPARSE_ERROR3, if the source array has more than one attribute.
 *
 * @par Notes:
 *   - The build_sparse operator can only take as input bounded dimensions.
 *
 */
class LogicalBuildSparse: public LogicalOperator
{
public:
    LogicalBuildSparse(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_SCHEMA()
        ADD_PARAM_EXPRESSION("void")
        ADD_PARAM_EXPRESSION("bool")
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 0);
        ArrayDesc desc = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        if (desc.getAttributes().size() != 1
                && (desc.getAttributes().size() != 2 || desc.getEmptyBitmapAttribute() == NULL))
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_SPARSE_ERROR3,
                                       _parameters[0]->getParsingContext());

        if (desc.getName().size() == 0)
        {
            desc.setName("build_sparse");
        }

        // Check dimensions
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size();  i < n; i++)
        {
            if (dims[i].getLength() == INFINITE_LENGTH)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_SPARSE_ERROR4,
                                           _parameters[0]->getParsingContext());
        }
        return desc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBuildSparse, "build_sparse")

} //namespace
