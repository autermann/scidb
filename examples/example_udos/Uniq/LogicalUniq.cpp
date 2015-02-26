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

/**
 * @file LogicalUniq.cpp
 * An example operator that removes duplicates from a sorted one-dimensional array. In other words, it works just like
 * the Unix "uniq" command. This operator presents a more advanced algorithm and introduces some concepts in writing
 * data to large output arrays and data distribution.
 * <br>
 * <br>
 * This operator provides some capabilities that stock SciDB does not currently have, and may prove very useful in
 * many real-world cases.
 *
 * @brief The operator: uniq()
 *
 * @par Synopsis: uniq (input_array [,'chunk_size=CHUNK_SIZE'] )
 *
 * @par Examples:
 *   <br> uniq (sorted_array)
 *   <br> store ( uniq ( sort ( project (big_array, string_attribute) ), 'chuk_size=100000'), string_attribute_index )
 *
 * @par Summary:
 *   <br>
 *   The input array must have a single attribute of any type and a single dimension. The data in the input array must
 *   be sorted. The operator is built to accept the output produced by sort() with a single attribute. The output array
 *   shall have the same attribute with the dimension i starting at 0 and chunk size of 1 million. An optional
 *   chunk_size parameter may be used to set a different output chunk size.
 *   <br>
 *   <br>
 *   Data is compared using a simple binary comparison of underlying memory. Null values are discarded from the output.
 *   The operator may be extended to use a system-registered equality function comparison. It may also be extended to
 *   handle multiple attributes. Furthermore, one can add an optional strict check to make sure the input data is, in
 *   fact, dense and sorted, and throw an early error if that's not the case.
 *
 * @par Input: array <single_attribute: INPUT_ATTRIBUTE_TYPE> [single_dimension= *]
 *
 * @par Output array:
 *   <br> <
 *   <br>   single_attribute: INPUT_ATTRIBUTE_TYPE
 *   <br> >
 *   <br> [
 *   <br>   i = 0:*,CHUNK_SIZE,0
 *   <br> ]
 *   <br>
 *
 * @see PhysicalUniq.cpp for a description of the algorithm.
 * The code assumes familiarity with the concepts described in hello_instances nad instance_stats. Consider reading
 * those operators first.
 * @see LogicalHelloInstances.cpp
 * @see LogicalInstanceStats.cpp
 */

#include <query/Operator.h>
#include "UniqSettings.h"

namespace scidb
{

class LogicalUniq : public LogicalOperator
{
public:
    LogicalUniq(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        /* We use the same Settings pattern as described in InstanceStatsSettings.h, with the new class UniqSettings */
        ADD_PARAM_VARIES()
    }

    vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(vector< ArrayDesc> const& schemas)
    {
        vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < UniqSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT(TID_STRING));
        }
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        if (inputSchema.getAttributes(true).size() != 1)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                  << "Operator uniq only accepts an input with a single attribute";
        }
        if (inputSchema.getDimensions().size() != 1 || inputSchema.getDimensions()[0].getStartMin() != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                  << "Operator uniq only accepts an input with a single dimension that starts at 0";
        }
        AttributeDesc const& inputAttribute = inputSchema.getAttributes()[0];
        UniqSettings settings (_parameters, true, query);
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0,
                                                  inputAttribute.getName(),
                                                  inputAttribute.getType(),
                                                  0, //no longer nullable
                                                  inputAttribute.getDefaultCompressionMethod())); //Note:
        /* The compression feature (beyond RLE) is rarely used but here we carry the value over from the input.
         * It only has an effect if it is not set to NONE (default) and the array is stored later in the query.
         */
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, MAX_COORDINATE, settings.outputChunkSize(), 0));
        return ArrayDesc(inputSchema.getName(), outputAttributes, outputDimensions);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalUniq, "uniq");

} //namespace scidb
