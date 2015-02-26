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
 * @file LogicalExpression.h
 *
 * @brief Instances of logical expressions
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author roman.simakov@gmail.com
 */

#ifndef LOGICALEXPRESSION_H_
#define LOGICALEXPRESSION_H_

#include <boost/shared_ptr.hpp>

#include "array/Metadata.h"
#include "array/Array.h"
#include "query/TypeSystem.h"

namespace scidb
{

class ParsingContext;

/**
 * Base class for logical expressions
 */
class LogicalExpression
{
public:
	LogicalExpression(const boost::shared_ptr<ParsingContext>& parsingContext):
		_parsingContext(parsingContext)
	{}

    virtual ~LogicalExpression() {} /**< To make base class virtual */

	boost::shared_ptr<ParsingContext> getParsingContext() const
	{
		return _parsingContext;
	}

    virtual void toString (std::ostringstream &str, int indent = 0) const
	{
		for ( int i = 0; i < indent; i++)
		{
			str<<" ";
		}
		str<<"[logicalExpression]\n";
	}

private:
	boost::shared_ptr<ParsingContext> _parsingContext;
};

class AttributeReference: public LogicalExpression
{
public:
    AttributeReference(const boost::shared_ptr<ParsingContext>& parsingContext,
    	const std::string& arrayName, const std::string& attributeName):
    	LogicalExpression(parsingContext), _arrayName(arrayName), _attributeName(attributeName)
    {
    }

	const std::string& getArrayName() const	{
		return _arrayName;
	}

	const std::string& getAttributeName() const	{
		return _attributeName;
	}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream &str, int indent = 0) const
	{
		for ( int i = 0; i < indent; i++)
		{
			str<<" ";
		}
		str<<"[attributeReference] array "<<_arrayName<<" attr "<<_attributeName<<"\n";
	}


private:
	std::string _arrayName;
	std::string _attributeName;
};

class Constant : public LogicalExpression
{
public:
	Constant(const boost::shared_ptr<ParsingContext>& parsingContext, const  Value& value,
		const  TypeId& type): LogicalExpression(parsingContext), _value(value), _type(type)
	{
	}

	const  Value& getValue() const {
		return _value;
	}

	const  TypeId& getType() const {
        return _type;
    }


    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream &str, int indent = 0) const
	{
		for ( int i = 0; i < indent; i++)
		{
			str<<" ";
		}
		str<<"[constant] type "<<_type<<" value "<< ValueToString(_type,_value)<<"\n";
	}



private:
	 Value _value;
	 TypeId _type;
};


class Function : public LogicalExpression
{
public:
	Function(const boost::shared_ptr<ParsingContext>& parsingContext, const std::string& function,
		const std::vector<boost::shared_ptr<LogicalExpression> >& args):
		LogicalExpression(parsingContext), _function(function), _args(args)
	{
	}

	const std::string& getFunction() const {
		return _function;
	}

	const std::vector<boost::shared_ptr<LogicalExpression> >& getArgs() const {
		return _args;
	}

    virtual void toString (std::ostringstream &str, int indent = 0) const
	{
		for ( int i = 0; i < indent; i++)
		{
			str<<" ";
		}
		str<<"[function] "<<_function<<" args:\n";
		for ( size_t i = 0; i < _args.size(); i ++)
		{
			_args[i]->toString(str, indent + 1);
		}
	}

private:
	std::string _function;
	std::vector<boost::shared_ptr<LogicalExpression> > _args;
};


} // namespace scidb

#endif /* LOGICALEXPRESSION_H_ */
