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
 * @file IndexLookupSettings.h
 * The settings structure for the index_lookup operator.
 * @see InstanceStatsSettings.h
 * @author apoliakov@paradigm4.com
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef INDEX_LOOKUP_SETTINGS
#define INDEX_LOOKUP_SETTINGS

namespace scidb
{

/*
 * Settings for the IndexLookup operator.
 */
class IndexLookupSettings
{
private:
    ArrayDesc const& _inputSchema;
    ArrayDesc const& _indexSchema;
    AttributeID const _inputAttributeId;
    string const _inputAttributeName;
    string _outputAttributeName;
    bool _outputAttributeNameSet;
    size_t _memoryLimit;
    bool _memoryLimitSet;

    void parseMemoryLimit(string const& parameterString, string const& paramHeader)
    {
        if(_memoryLimitSet)
        {
            ostringstream error;
            error<<"The '"<<paramHeader<<"' parameter cannot be set more than once";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        string paramContent = parameterString.substr(paramHeader.size());
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
        _memoryLimit = sval * MB;
        _memoryLimitSet = true;
    }

    void setOutputAttributeName(shared_ptr<OperatorParam>const& param)
    {
        if(_outputAttributeNameSet)
        {
            ostringstream error;
            error<<"The output attribute name cannot be set more than once";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        //extracting the identifier that's been parsed and prepared by scidb
        _outputAttributeName = ((shared_ptr<OperatorParamReference> const&)param)->getObjectName();
        _outputAttributeNameSet = true;
    }

    void checkInputSchemas()
    {
        //throw an error if the schemas don't satisfy us
        ostringstream err;
        if (_indexSchema.getDimensions().size() > 1 ||
            _indexSchema.getAttributes(true).size() > 1)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                   << "The index array must have only one attribute and one dimension";
        }
        AttributeDesc const& inputAttribute = _inputSchema.getAttributes()[_inputAttributeId];
        if (inputAttribute.getName() != _inputAttributeName)
        {
            err << _inputAttributeName<<" is not an attribute in the input array "<<_inputSchema.getName();
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
        }
        AttributeDesc const& indexAttribute = _indexSchema.getAttributes()[0];
        if (inputAttribute.getType() != indexAttribute.getType())
        {
            err << "The attribute type of "<<_inputAttributeName<<" ("<<inputAttribute.getType()
                <<") does not match the index type of " <<indexAttribute.getName()<<" ("<<indexAttribute.getType()<<")";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
        }
        if (indexAttribute.isNullable())
        {
            err << "The index attribute "<<indexAttribute.getName()<<" cannot be nullable";
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
        }
    }

    void checkOutputAttributeName()
    {
        //make sure the output attribute name doesn't match the name of some other existing attribute
        for(AttributeID i =0; i<_inputSchema.getAttributes().size(); ++i)
        {
            if(_inputSchema.getAttributes()[i].getName() == _outputAttributeName)
            {
                ostringstream err;
                err << "The output attribute name "<<_outputAttributeName
                    <<" already appears in the input array; please provide a different name";
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
            }
        }
    }

public:
    static const size_t MAX_PARAMETERS = 3;

    IndexLookupSettings(ArrayDesc const& inputSchema,
                        ArrayDesc const& indexSchema,
                        vector<shared_ptr<OperatorParam> > const& operatorParameters,
                        bool logical,
                        shared_ptr<Query>& query):
        _inputSchema            (inputSchema),
        _indexSchema            (indexSchema),
        _inputAttributeId       (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectNo()),
        _inputAttributeName     (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectName()),
        _outputAttributeName    (_inputSchema.getAttributes()[_inputAttributeId].getName() + "_index"),
        _outputAttributeNameSet (false),
        _memoryLimit            ((((size_t) Config::getInstance()->getOption<int>(CONFIG_MEM_ARRAY_THRESHOLD)) * MB)),
        _memoryLimitSet         (false)

    {
        checkInputSchemas();
        string const memLimitHeader = "memory_limit=";
        size_t nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to UniqSettings";
        }
        for (size_t i = 1; i<nParams; ++i) //parameter 0 is already checked above
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            if (param->getParamType()== PARAM_ATTRIBUTE_REF)
            {
                setOutputAttributeName(param);
            }
            else
            {
                string parameterString;
                if (logical)
                {
                    parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->
                                               getExpression(),query, TID_STRING).getString();
                }
                else
                {
                    parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->
                                      getExpression()->evaluate().getString();
                }
                if (starts_with(parameterString, memLimitHeader))
                {
                    parseMemoryLimit(parameterString, memLimitHeader);
                }
                else
                {
                    ostringstream error;
                    error<<"Unrecognized parameter: '"<<parameterString<<"'";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
                }
            }
        }
        checkOutputAttributeName();
    }

    /**
     * @return the memory limit (converted to bytes)
     */
    size_t getMemoryLimit() const
    {
        return _memoryLimit;
    }

    /**
     * @return the name of the output attribute
     */
    string const& getOutputAttributeName() const
    {
        return _outputAttributeName;
    }

    /**
     * @return the id of the input attribute
     */
    AttributeID getInputAttributeId() const
    {
        return _inputAttributeId;
    }
};

}

#endif //INDEX_LOOKUP_SETTINGS
