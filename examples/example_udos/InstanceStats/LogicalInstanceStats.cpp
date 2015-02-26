/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
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
 * @file LogicalInstanceStats.cpp
 * An example operator that outputs interesting statistics for a single-attribute array with a double attribute. This
 * is a simple UDO designed to provide an example for reading data from an input array, processing multiple optional
 * parameters, logging, and exchanging messages between instances. Running the operator illustrates how SciDB
 * distributes data between instances. The operator may be extended to become a more general data distribution, size and
 * statistics tool.
 *
 * @brief The operator: instance_stats()
 *
 * @par Synopsis: instance_stats( input_array
 *                                [,'log=true/false']
 *                                [,'include_overlap=true/false']
 *                                [,'global=true/false'] )
 *
 * @par Examples:
 *   <br> instance_stats (my_array, 'log=true', 'global=true')
 *   <br> instance_stats (project(big_array, double_attribute), 'include_overlap=true', 'log=true')
 *
 * @par Summary:
 *   <br>
 *   The input array must have one double attribute and any number of dimensions. There are 3 optional string "flag"
 *   parameters: log, include_overlap and global. They are all set to false by default. If log is true, all the
 *   local data from the input array is saved to scidb.log on each instance. If include_overlap is true, the operator
 *   will include data from the array overlaps into the calculation, otherwise overlaps are ignored. If global is true,
 *   the operator returns a single summary for the entire array. Else, it returns a per-instance summary of the data
 *   located on each instance.
 *
 * @par Input: array <attribute:double> [*]
 *
 * @par Output array:
 *   <br> If global is true:
 *   <br> <
 *   <br>   num_chunks: uint32          --the total number of chunks in the array
 *   <br>   num_cells: uint64           --the total number of cells in the array
 *   <br>   num_non_null_cells: uint64  --the total number of cells that are not null
 *   <br>   average_value: double       --the average value of all the cells (null if none)
 *   <br> >
 *   <br> [
 *   <br>   i = 0:0,1,0                 --single cell
 *   <br> ]
 *   <br>
 *   <br> If global is false:
 *   <br> <
 *   <br>   num_chunks: uint32          --the total number of chunks on this instance
 *   <br>   num_cells: uint64           --the total number of cells on this instance
 *   <br>   num_non_null_cells: uint64  --the total number of cells that are not null on this instance
 *   <br>   average_value: double       --the average value of all the cells on this instance (null if none)
 *   <br> >
 *   <br> [
 *   <br>   instance_no = 0:INSTANCE_COUNT,1,0  --one cell per instance
 *   <br> ]
 *
 * The code assumes familiarity with the concepts described in hello_instances. Consider reading that operator first if
 * you have not already.
 * @see LogicalHelloInstances.cpp
 * @author apoliakov@paradigm4.com
 */

#include <query/Operator.h>
#include "InstanceStatsSettings.h"

namespace scidb
{

class LogicalInstanceStats : public LogicalOperator
{
public:
    LogicalInstanceStats(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        /* Tell SciDB we accept one input array. All input arrays must be listed first. */
        ADD_PARAM_INPUT()

        /* Tell SciDB we accept a variable-sized list of parameters in addition to the input array */
        ADD_PARAM_VARIES()
    }

    /**
     * Given the schemas of the input arrays and the parameters supplied so far, return a list of all the possible
     * types of the next parameter. This is an optional function to be overridden only in operators that accept optional
     * parameters.
     * @param schemas the shapes of the input arrays
     * @return the list of possible types of the next parameters
     */
    vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(vector< ArrayDesc> const& schemas)
    {
        /* A list of all possble things that the next parameter could be. */
        vector<shared_ptr<OperatorParamPlaceholder> > res;

        /* the next parameter may be "end of parameters" - that's always true */
        res.push_back(END_OF_VARIES_PARAMS());

        /* If we haven't reached the max number of parameters, the next parameter may be a string constant */
        if (_parameters.size() < InstanceStatsSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT(TID_STRING));
        }
        return res;
    }

    /**
     * @note all the parameters are assembled in the _parameters member variable
     */
    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];

        /* Throw an error if the input array schema does not match our needs.
         * This error will cleanly abort the entire query. Throwing it here is useful as query execution has not yet
         * begun. The error code system uses a short code, followed by a long code, followed by a human-readable error
         * description. The codes are numeric values created to facilitate error recognition by client software. Many
         * error codes are available and new error codes may be added from plugins.
         *
         * The call to getAttributes(true) excludes the empty tag if any.
         */
        if (inputSchema.getAttributes(true).size() != 1 ||
            inputSchema.getAttributes(true)[0].getType() != TID_DOUBLE)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                  << "Operator instance_stats accepts an array with a single attribute of type double";
        }

        /* Construct the settings object that parses and validates the other parameters. */
        InstanceStatsSettings settings (_parameters, true, query);

        /* Make the output schema based on the parameters. Here we illustrate the potential flexibility of operator
         * behavior, based on arguments.
         */
        Attributes outputAttributes;

        /* Note: attribute 3 may contain null values */
        outputAttributes.push_back( AttributeDesc(0, "num_chunks", TID_UINT32, 0, 0));
        outputAttributes.push_back( AttributeDesc(1, "num_cells", TID_UINT64, 0, 0));
        outputAttributes.push_back( AttributeDesc(2, "num_non_null_cells", TID_UINT64, 0, 0));
        outputAttributes.push_back( AttributeDesc(3, "average_value", TID_DOUBLE, AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        if(settings.global())
        {
            outputDimensions.push_back(DimensionDesc("i", 0, 0, 1, 0));
        }
        else
        {
            outputDimensions.push_back(DimensionDesc("instance_no", 0, query->getInstancesCount(), 1, 0));
        }
        return ArrayDesc("instance_stats", outputAttributes, outputDimensions);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalInstanceStats, "instance_stats");

} //namespace scidb
