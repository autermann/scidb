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
 * @file LogicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Save operator for saveing data from external files into array
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"


using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: save().
 *
 * @par Synopsis:
 *   save( srcArray, file, instanceId = -2, format = 'store' )
 *
 * @par Summary:
 *   Saves the data in an array to a file.
 *
 * @par Input:
 *   - srcArray: the source array to save from.
 *   - file: the file to save to.
 *   - instanceId: which instance
 *   - format: what format.
 *
 * @par Output array:
 *   the srcArray is returned
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
/**
 * Must be called as SAVE('existing_array_name', '/path/to/file/on/instance')
 */
class LogicalSave: public LogicalOperator
{
public:
    LogicalSave(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
		ADD_PARAM_CONSTANT("string")//0
		ADD_PARAM_VARIES();          //2
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
          case 0:
            assert(false);
            break;
          case 1:
            res.push_back(PARAM_CONSTANT("int64"));
            break;
          case 2:
            res.push_back(PARAM_CONSTANT("string"));
            break;
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        assert(_parameters.size() >= 1);
        
        if (_parameters.size() >= 3) { 
            string const& format = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                                query, TID_STRING).getString();
            if (format[0] != '(' 
                && !format.empty()
                && compareStringsIgnoreCase(format, "opaque") != 0
                && compareStringsIgnoreCase(format, "text") != 0
                && compareStringsIgnoreCase(format, "auto") != 0
                && compareStringsIgnoreCase(format, "text") != 0
                && compareStringsIgnoreCase(format, "csv") != 0
                && compareStringsIgnoreCase(format, "csv+") != 0
                && compareStringsIgnoreCase(format, "lcsv+") != 0
                && compareStringsIgnoreCase(format, "lsparse") != 0
                && compareStringsIgnoreCase(format, "sparse") != 0
                && compareStringsIgnoreCase(format, "dense") != 0
                && compareStringsIgnoreCase(format, "store") != 0
                && compareStringsIgnoreCase(format, "dcsv") != 0)
            {
                throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNSUPPORTED_FORMAT, _parameters[2]->getParsingContext()) << format;
            }
        }


        return inputSchemas[0];
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSave, "save")


} //namespace
