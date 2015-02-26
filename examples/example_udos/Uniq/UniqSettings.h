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

/*
 * @file UniqSettings.h
 * A common settings structure for the uniq operator. This class uses the same settings pattern as introduced in
 * InstanceStatsSettings.h
 * @author apoliakov@paradigm4.com
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef UNIQ_SETTINGS
#define UNIQ_SETTINGS

namespace scidb
{

/**
 * Very simple; has only one optional output parameter - the output chunk size.
 */
class UniqSettings
{
private:
    size_t _outputChunkSize;

public:
    static const size_t MAX_PARAMETERS = 1;

    UniqSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 shared_ptr<Query>& query):
       _outputChunkSize(1000000)
    {
        string const chunkSizeParamHeader = "chunk_size=";
        size_t nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to UniqSettings";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            string parameterString;
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if (starts_with(parameterString, chunkSizeParamHeader))
            {
                string paramContent = parameterString.substr(chunkSizeParamHeader.size());
                trim(paramContent);
                int64_t sval;
                try
                {
                    sval = lexical_cast<int64_t> (paramContent);
                }
                catch (bad_lexical_cast const& exn)
                {
                    string err = "The parameter " + parameterString + " could not be parsed into an integer value";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err;
                }
                if (sval <= 0)
                {
                    string err = "The parameter " + parameterString + " is not valid; must be a positive integer";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err;
                }
                _outputChunkSize = sval;
            }
            else
            {
                ostringstream error;
                error<<"Unrecognized parameter: '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
            }
        }
    }

    size_t outputChunkSize() const
    {
        return _outputChunkSize;
    }
};

}

#endif //UNIQ_SETTINGS
