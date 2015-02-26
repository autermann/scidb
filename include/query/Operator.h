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
 * @file Operator.h
 *
 * @author roman.simakov@gmail.com
 *
 * This file contains base classes for implementing operators and
 * registering them in operator library. The sample of implementation a new logical and physical
 * operators see in ops/example folder. In order to declare you own operator just copy this
 * folder with new operator name and re-implement methods inside.
 *
 * Note: Remember about versions of API before changes.
 */

#ifndef OPERATOR_H_
#define OPERATOR_H_

#include <iostream>
#include <vector>
#include <string>
#include <stdio.h>
#include <utility>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/format.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/export.hpp>

#include "array/Array.h"
#include "array/MemArray.h"
#include "query/TypeSystem.h"
#include "query/LogicalExpression.h"
#include "query/Expression.h"
#include "query/Query.h"
#include "system/Config.h"
#include "../src/system/SciDBConfigOptions.h"
#include "../src/smgr/io/DimensionIndex.h"

namespace scidb
{

class Query;
class DistributionMapper;

#define PARAMETERS public: void initParameters()

/*
 * Don't forget update HELP operator if some new placeholders added!
 */
enum OperatorParamPlaceholderType
{
        PLACEHOLDER_INPUT            = 1,
        PLACEHOLDER_ARRAY_NAME       = 2,
        PLACEHOLDER_ATTRIBUTE_NAME   = 4,
        PLACEHOLDER_DIMENSION_NAME   = 8,
        PLACEHOLDER_CONSTANT         = 16,
        PLACEHOLDER_EXPRESSION       = 32,
        PLACEHOLDER_VARIES           = 64,
        PLACEHOLDER_SCHEMA           = 128,
        PLACEHOLDER_AGGREGATE_CALL   = 256,
        PLACEHOLDER_END_OF_VARIES    = 512 // Must be last!
};

enum PlaceholderArrayName
{
    PLACEHOLDER_ARRAY_NAME_VERSION = 1,
    PLACEHOLDER_ARRAY_NAME_INDEX_NAME = 2
};

#define stringify(name) #name

static const char *OperatorParamPlaceholderTypeNames[] =
{
        stringify(PLACEHOLDER_INPUT),
        stringify(PLACEHOLDER_ARRAY_NAME),
        stringify(PLACEHOLDER_ATTRIBUTE_NAME),
        stringify(PLACEHOLDER_DIMENSION_NAME),
        stringify(PLACEHOLDER_CONSTANT),
        stringify(PLACEHOLDER_EXPRESSION),
        stringify(PLACEHOLDER_VARIES),
        stringify(PLACEHOLDER_SCHEMA),
        stringify(PLACEHOLDER_AGGREGATE_CALL),
        stringify(PLACEHOLDER_END_OF_VARIES)
};

struct OperatorParamPlaceholder
{
public:
        OperatorParamPlaceholder(
                OperatorParamPlaceholderType placeholderType,
                Type requiredType,
                bool inputScheme,
                int flags
                ) :
                _placeholderType(placeholderType),
                _requiredType(requiredType),
                _inputSchema(inputScheme),
                _flags(flags)
        {}

    virtual ~OperatorParamPlaceholder() {}

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
        int typeIndex = 0, type = _placeholderType;
        while ((type >>= 1) != 0) {
            typeIndex += 1;
        }
                str<<"[opParamPlaceholder] "<<OperatorParamPlaceholderTypeNames[typeIndex]
                   <<" requiredType "<<_requiredType.name()<<" ischeme "<<_inputSchema<<"\n";
        }

        OperatorParamPlaceholderType getPlaceholderType() const
        {
            return _placeholderType;
        }

        const Type& getRequiredType() const
        {
            return _requiredType;
        }

        bool isInputSchema() const
        {
            return _inputSchema;
        }

        int getFlags() const
        {
            return _flags;
        }

private:
        OperatorParamPlaceholderType _placeholderType;

        Type _requiredType;

        bool _inputSchema;

        int _flags;
};

typedef std::vector<boost::shared_ptr<OperatorParamPlaceholder> > OperatorParamPlaceholders;

#define PARAM_IN_ARRAY_NAME() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define PARAM_IN_ARRAY_NAME2(flags) \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        flags))

#define ADD_PARAM_IN_ARRAY_NAME() \
        addParamPlaceholder(PARAM_IN_ARRAY_NAME());

#define ADD_PARAM_IN_ARRAY_NAME2(flags) \
        addParamPlaceholder(PARAM_IN_ARRAY_NAME2(flags));

#define PARAM_OUT_ARRAY_NAME() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_OUT_ARRAY_NAME() \
        addParamPlaceholder(PARAM_OUT_ARRAY_NAME());


#define PARAM_INPUT() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_INPUT,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define ADD_PARAM_INPUT() \
        addParamPlaceholder(PARAM_INPUT());

#define ADD_PARAM_VARIES() \
        addParamPlaceholder(\
        boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
            scidb::PLACEHOLDER_VARIES,\
            scidb::TypeLibrary::getType("void"),\
            false,\
            0)));

#define PARAM_OUT_ATTRIBUTE_NAME(type) \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ATTRIBUTE_NAME,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_OUT_ATTRIBUTE_NAME(type) \
        addParamPlaceholder(PARAM_OUT_ATTRIBUTE_NAME(type));

#define PARAM_IN_ATTRIBUTE_NAME(type) \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ATTRIBUTE_NAME,\
        scidb::TypeLibrary::getType(type),\
        true,\
        0))

#define ADD_PARAM_IN_ATTRIBUTE_NAME(type) \
        addParamPlaceholder(PARAM_IN_ATTRIBUTE_NAME(type));

#define PARAM_IN_DIMENSION_NAME() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_DIMENSION_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define ADD_PARAM_IN_DIMENSION_NAME() \
        addParamPlaceholder(PARAM_IN_DIMENSION_NAME());

#define PARAM_OUT_DIMENSION_NAME() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_DIMENSION_NAME,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_OUT_DIMENSION_NAME() \
        addParamPlaceholder(PARAM_OUT_DIMENSION_NAME());


#define PARAM_EXPRESSION(type) \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_EXPRESSION,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_EXPRESSION(type) \
        addParamPlaceholder(PARAM_EXPRESSION(type));

#define PARAM_CONSTANT(type) \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_CONSTANT,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_CONSTANT(type) \
        addParamPlaceholder(PARAM_CONSTANT(type));

#define PARAM_SCHEMA() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_SCHEMA,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_SCHEMA() \
    addParamPlaceholder(PARAM_SCHEMA());

#define PARAM_AGGREGATE_CALL() \
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_AGGREGATE_CALL,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_AGGREGATE_CALL() \
    addParamPlaceholder(PARAM_AGGREGATE_CALL());

#define END_OF_VARIES_PARAMS()\
    boost::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_END_OF_VARIES,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

enum OperatorParamType
{
        PARAM_UNKNOWN,
        PARAM_ARRAY_REF,
        PARAM_ATTRIBUTE_REF,
        PARAM_DIMENSION_REF,
        PARAM_LOGICAL_EXPRESSION,
        PARAM_PHYSICAL_EXPRESSION,
        PARAM_SCHEMA,
        PARAM_AGGREGATE_CALL,
    PARAM_ASTERISK
};

class OperatorParam
{
public:
        OperatorParam() :
                _paramType(PARAM_UNKNOWN)
        {
        }

        OperatorParam(OperatorParamType paramType, const boost::shared_ptr<ParsingContext>& parsingContext) :
                _paramType(paramType),
                _parsingContext(parsingContext)
        {}


        OperatorParamType getParamType() const
        {
                return _paramType;
        }

        boost::shared_ptr<ParsingContext> getParsingContext() const
        {
                return _parsingContext;
        }

        //Must be strongly virtual for successful serialization
        virtual ~OperatorParam()
        {
        }

protected:
        OperatorParamType _paramType;
        boost::shared_ptr<ParsingContext> _parsingContext;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _paramType;
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
        str<<"[param] type "<<_paramType<<"\n";
        }
};

class OperatorParamReference: public OperatorParam
{
public:
        OperatorParamReference() :
                OperatorParam(),
                _arrayName(""),
                _objectName(""),
                _inputNo(-1),
                _objectNo(-1),
                _inputScheme(false)
        {
        }

        OperatorParamReference(OperatorParamType paramType, const boost::shared_ptr<ParsingContext>& parsingContext,
                const std::string& arrayName, const std::string& objectName, bool inputScheme):
                OperatorParam(paramType, parsingContext),
                _arrayName(arrayName),
                _objectName(objectName),
                _inputNo(-1),
                _objectNo(-1),
                _inputScheme(inputScheme)
        {}

        const std::string& getArrayName() const
        {
                return _arrayName;
        }

        const std::string& getObjectName() const
        {
                return _objectName;
        }

        int32_t getInputNo() const
        {
                return _inputNo;
        }

        int32_t getObjectNo() const
        {
                return _objectNo;
        }

        void setInputNo(int32_t inputNo)
        {
                _inputNo = inputNo;
        }

        void setObjectNo(int32_t objectNo)
        {
                _objectNo = objectNo;
        }

        bool isInputScheme() const
        {
                return _inputScheme;
        }

private:
        std::string _arrayName;
        std::string _objectName;

        int32_t _inputNo;
        int32_t _objectNo;

        bool _inputScheme;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _arrayName;
        ar & _objectName;
        ar & _inputNo;
        ar & _objectNo;
        ar & _inputScheme;
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
        str<<"object "<<_objectName
                   <<" inputNo "<<_inputNo
                   <<" objectNo "<<_objectNo
                   <<" inputScheme "<<_inputScheme
                   <<"\n";
        }

};

class OperatorParamArrayReference: public OperatorParamReference
{
public:
    OperatorParamArrayReference() :
        OperatorParamReference()
    {
        _paramType = PARAM_ARRAY_REF;
    }

    OperatorParamArrayReference(const boost::shared_ptr<ParsingContext>& parsingContext,
        const std::string& arrayName, const std::string& objectName, bool inputScheme,
        VersionID version = 0,
        std::string index = ""):
        OperatorParamReference(PARAM_ARRAY_REF, parsingContext, arrayName, objectName, inputScheme),
        _version(version),
        _index(index)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _version;
        ar & _index;
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
        str<<"[paramArrayReference] ";
        OperatorParamReference::toString(str,indent);
    }

    VersionID getVersion() const;

    const std::string& getIndex() const;

private:
    VersionID _version;
    std::string _index;
};

class OperatorParamAttributeReference: public OperatorParamReference
{
public:
        OperatorParamAttributeReference() :
                OperatorParamReference(),
        _sortAscent(true)
        {
                _paramType = PARAM_ATTRIBUTE_REF;
        }

        OperatorParamAttributeReference(const boost::shared_ptr<ParsingContext>& parsingContext,
                const std::string& arrayName, const std::string& objectName, bool inputScheme):
                OperatorParamReference(PARAM_ATTRIBUTE_REF, parsingContext, arrayName, objectName, inputScheme),
        _sortAscent(true)
        {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _sortAscent;
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
                str<<"[paramAttributeReference] ";
                OperatorParamReference::toString(str,indent);
        }

    bool getSortAscent() const
    {
        return _sortAscent;
    }

    void setSortAscent(bool sortAscent)
    {
        _sortAscent = sortAscent;
    }

private:
    //Sort quirk
    bool _sortAscent;
};

class OperatorParamDimensionReference: public OperatorParamReference
{
public:
        OperatorParamDimensionReference() :
                OperatorParamReference()
        {
                _paramType = PARAM_DIMENSION_REF;
        }

        OperatorParamDimensionReference(const boost::shared_ptr<ParsingContext>& parsingContext,
                const std::string& arrayName, const std::string& objectName, bool inputScheme):
                OperatorParamReference(PARAM_DIMENSION_REF, parsingContext, arrayName, objectName, inputScheme)
        {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
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
                str<<"[paramDimensionReference] ";
                OperatorParamReference::toString(str,indent);
        }
};

class OperatorParamLogicalExpression: public OperatorParam
{
public:
        OperatorParamLogicalExpression() :
                OperatorParam()
        {
                _paramType = PARAM_LOGICAL_EXPRESSION;
        }

        OperatorParamLogicalExpression(const boost::shared_ptr<ParsingContext>& parsingContext,
                const boost::shared_ptr<LogicalExpression>& expression,  Type expectedType,
                bool constant = false):
                OperatorParam(PARAM_LOGICAL_EXPRESSION, parsingContext),
                _expression(expression),
                _expectedType(expectedType),
                _constant(constant)
                {}

        boost::shared_ptr<LogicalExpression> getExpression() const
        {
                return _expression;
        }

        const  Type& getExpectedType() const
        {
                return _expectedType;
        }

        bool isConstant() const
        {
                return _constant;
        }

private:
        boost::shared_ptr<LogicalExpression> _expression;

         Type _expectedType;

        bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        assert(0);
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
                str<<"[paramLogicalExpression] type "<<_expectedType.name()<<" const "<<_constant<<"\n";
                _expression->toString(str, indent+1);
        }

};

class OperatorParamPhysicalExpression: public OperatorParam
{
public:
        OperatorParamPhysicalExpression() :
                OperatorParam()
        {
                _paramType = PARAM_PHYSICAL_EXPRESSION;
        }

        OperatorParamPhysicalExpression(const boost::shared_ptr<ParsingContext>& parsingContext,
                const boost::shared_ptr<Expression>& expression, bool constant = false):
                OperatorParam(PARAM_PHYSICAL_EXPRESSION, parsingContext),
                _expression(expression),
                _constant(constant)
                {}

        boost::shared_ptr<Expression> getExpression() const
        {
                return _expression;
        }

        bool isConstant() const
        {
                return _constant;
        }

private:
        boost::shared_ptr<Expression> _expression;

        bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _expression;
        ar & _constant;
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
                str<<"[paramPhysicalExpression] const "<<_constant<<"\n";
                _expression->toString(str, indent+1);
        }

};


class OperatorParamSchema: public OperatorParam
{
public:
    OperatorParamSchema() :
        OperatorParam()
    {
        _paramType = PARAM_SCHEMA;
    }

    OperatorParamSchema(const boost::shared_ptr<ParsingContext>& parsingContext,
        const ArrayDesc& schema):
        OperatorParam(PARAM_SCHEMA, parsingContext),
        _schema(schema)
        {}

    const ArrayDesc& getSchema() const
    {
        return _schema;
    }

private:
    ArrayDesc _schema;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _schema;
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
        str<<"[paramSchema] " << _schema <<"\n";
    }

};

class OperatorParamAggregateCall: public OperatorParam
{
public:
    OperatorParamAggregateCall() :
        OperatorParam()
    {
        _paramType = PARAM_AGGREGATE_CALL;
    }

    OperatorParamAggregateCall(const boost::shared_ptr<ParsingContext>& parsingContext,
                               const std::string& aggregateName,
                               boost::shared_ptr <OperatorParam> const& inputAttribute,
                               const std::string& alias):
        OperatorParam(PARAM_AGGREGATE_CALL, parsingContext),
        _aggregateName(aggregateName),
        _inputAttribute(inputAttribute),
        _alias(alias)
    {}

    inline std::string const& getAggregateName() const
    {
        return _aggregateName;
    }

    inline boost::shared_ptr<OperatorParam> const& getInputAttribute() const
    {
        return _inputAttribute;
    }

    inline void setAlias(const string& alias)
    {
        _alias = alias;
    }

    inline std::string const& getAlias() const
    {
        return _alias;
    }

private:
    std::string _aggregateName;
    boost::shared_ptr <OperatorParam> _inputAttribute;
    std::string _alias;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _aggregateName;
        ar & _inputAttribute;
        ar & _alias;
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
        str<<"[paramAggregateCall] " << _aggregateName << "\n" ;

        for ( int i = 0; i < indent+1; i++)
        {
            str<<" ";
        }
        str<<"input: ";
        _inputAttribute->toString(str);

        if (_alias.size() )
        {
            for ( int i = 0; i < indent+1; i++)
            {
                str<<" ";
            }
            str<<"alias "<<_alias<<"\n";
        }
    }
};

/**
 * @brief Little addition to aggregate call parameter. Mostly for built-in COUNT(*).
 */
class OperatorParamAsterisk: public OperatorParam
{
public:
    OperatorParamAsterisk():
        OperatorParam()
    {
        _paramType = PARAM_ASTERISK;
    }

    OperatorParamAsterisk(const boost::shared_ptr<ParsingContext>& parsingContext):
        OperatorParam(PARAM_ASTERISK, parsingContext)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
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
        str << "[paramAsterisk] *" << endl;
    }
};

/**
 *      This is pure virtual class for all logical operators. It provides API of logical operator.
 *      In order to add new logical operator we must inherit new class and implement all methods in it.
 *      Derived implementations are located in ops folder. See example there to know how to write new operators.
 */
class LogicalOperator
{
public:
    typedef std::vector< boost::shared_ptr<OperatorParam> > Parameters;

    struct Properties
    {
        bool ddl;
        bool exclusive;
        bool tile;
        Properties(): ddl(false), exclusive(false), tile(false)
        {
        }
    };

public:
    LogicalOperator(const std::string& logicalName,
                const std::string& aliasName = ""):
        _logicalName(logicalName),
        _aliasName(aliasName)
    {
    }

    virtual ~LogicalOperator(){}

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    /**
     * @return logical and physical operator names for the global phase if empty this is single-phase operator
     *
     * @todo actually logical name don't need for global operator because it will be inserted by
     * optimizer so need reconsider it later
     */
    const std::pair<std::string, std::string>& getGlobalOperatorName() const
    {
        return _globalOperatorName;
    }

    const Parameters& getParameters() const
        {
                return _parameters;
        }

    virtual void setParameters(const Parameters& parameters)
    {
        _parameters = parameters;
    }

        virtual void addParameter(const boost::shared_ptr<OperatorParam>& parameter)
        {
                _parameters.push_back(parameter);
        }

    virtual std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_UNHANDLED_VAR_PARAMETER) << _logicalName;
    }

    void setSchema(const ArrayDesc& schema)
    {
        _schema = schema;
        if (_aliasName.size() != 0) {
            _schema.setName(_aliasName);
        }
    }

    const ArrayDesc& getSchema() const
    {
        return _schema;
    }

    const std::string& getAliasName() const
    {
        return _aliasName;
    }

    void setAliasName(const std::string &alias)
    {
        _aliasName = alias;
    }

    const Properties& getProperties() const
    {
        return _properties;
    }

    virtual bool compileParamInTileMode(size_t paramNo) {
        return false;
    }

    virtual ArrayDesc inferSchema(std::vector< ArrayDesc>, boost::shared_ptr< Query> query) = 0;

    /**
     * This is where the logical operator can request array level locks for any of the arrays specified
     * in the operator parameters (or others)
     * @see scidb::Query::requstLock()
     * The default implementation requests scidb::SystemCatalog::LockDesc::RD locks for all arrays
     * mentioned in the query string.
     * The subclasses are expected to override this method if stricter locks are needed,
     * and it is recommended that the default scidb::LogicalOperator::inferArrayAccess() method is
     * also called in the overriden methods.
     * @note the locks are not acquired in this method - only requested
     * @param query the current query context
     */
    virtual void inferArrayAccess(boost::shared_ptr<Query>& query);

    void addParamPlaceholder(const boost::shared_ptr<OperatorParamPlaceholder> paramPlaceholder)
    {
        if (_paramPlaceholders.size() > 0 &&
                _paramPlaceholders[_paramPlaceholders.size() - 1]->getPlaceholderType() != PLACEHOLDER_INPUT &&
                paramPlaceholder->getPlaceholderType() == PLACEHOLDER_INPUT)
        {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INPUTS_MUST_BE_BEFORE_PARAMS) << _logicalName;
        }

        if (_paramPlaceholders.size() > 0 &&
                _paramPlaceholders[_paramPlaceholders.size() - 1]->getPlaceholderType() == PLACEHOLDER_VARIES)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_VAR_MUST_BE_AFTER_PARAMS) << _logicalName;
        }

        _paramPlaceholders.push_back(paramPlaceholder);
    }

    const OperatorParamPlaceholders& getParamPlaceholders() const
        {
        return _paramPlaceholders;
        }

    const std::string &getUsage() const
    {
        return _usage;
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

                str<<"[lOperator] "<<_logicalName<<" ddl "<<_properties.ddl<<"\n";
                for (size_t i = 0; i < _parameters.size(); i++)
                {
                        _parameters[i]->toString(str, indent+1);
                }

                for ( size_t i = 0; i < _paramPlaceholders.size(); i++)
                {
                        _paramPlaceholders[i]->toString(str,indent+1);
                }

                for ( int i = 0; i < indent; i++)
                {
                        str<<" ";
                }
                str<<"schema: "<<_schema<<"\n";
        }

protected:
    Parameters _parameters;
    Properties _properties;
    std::string _usage;
    std::pair<std::string, std::string> _globalOperatorName;

private:
    std::string _logicalName;
    ArrayDesc _schema;
    std::string _aliasName;
    OperatorParamPlaceholders _paramPlaceholders;
};


class DistributionMapper
{
public:
    //public for now
    DimensionVector _distOffsetVector;
    DimensionVector _distShapeVector;


private:
    DistributionMapper(DimensionVector const& offset, DimensionVector const& shape)
    {
        _distOffsetVector = offset;
        _distShapeVector = shape;
    }

public:

    virtual ~DistributionMapper()
    {}

    Coordinates translate (Coordinates const& input) const
    {
        assert(input.size() == _distOffsetVector.numDimensions());
        Coordinates result;

        for (size_t i = 0 ; i < input.size(); i++ )
        {
            result.push_back(input[i] + _distOffsetVector[i]);
        }

        return result;
    }

    DimensionVector getShapeVector() const
    {
        return _distShapeVector;
    }

    static boost::shared_ptr<DistributionMapper> createOffsetMapper(DimensionVector const& offset, DimensionVector const& shape)
    {
        return boost::shared_ptr<DistributionMapper> (new DistributionMapper(offset,shape));
    }

    //careful: this is not commutative
    boost::shared_ptr<DistributionMapper> combine(boost::shared_ptr<DistributionMapper> previous)
    {
        if (previous.get() == NULL)
        {
            return createOffsetMapper(_distOffsetVector, _distShapeVector);
        }

        DimensionVector newOffset = _distOffsetVector + previous->_distOffsetVector;
        DimensionVector newShape = previous->_distShapeVector;
        return createOffsetMapper(newOffset, newShape);
    }

    friend bool operator== (const DistributionMapper& lhs, const DistributionMapper& rhs)
    {
        return lhs._distOffsetVector == rhs._distOffsetVector &&
               lhs._distShapeVector == rhs._distShapeVector;
    }

    friend bool operator!= (const DistributionMapper& lhs, const DistributionMapper& rhs)
    {
        return ! (lhs == rhs);
    }

    friend std::ostream& operator<<(std::ostream& stream, const DistributionMapper& dm)
    {
        stream << "offset [" ;
        for (size_t i = 0; i < dm._distOffsetVector.numDimensions(); i++)
        {
            stream<<dm._distOffsetVector[i]<<" ";
        }

        stream << "]/[";
        for (size_t i = 0; i < dm._distShapeVector.numDimensions(); i++)
        {
            stream<<dm._distShapeVector[i]<<" ";
        }
        stream << "]";

        return stream;
    }
};


class ArrayDistribution
{
private:
    PartitioningSchema _partitioningSchema;
    boost::shared_ptr <DistributionMapper> _distMapper;
    int64_t _nodeId;

public:
    ArrayDistribution(PartitioningSchema ps = psRoundRobin,
                      boost::shared_ptr <DistributionMapper> distMapper = boost::shared_ptr<DistributionMapper>(),
                      int64_t nodeId = 0):
        _partitioningSchema(ps), _distMapper(distMapper), _nodeId(nodeId)
    {
        if(_distMapper.get() != NULL && _partitioningSchema == psUndefined)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNDEFINED_DISTRIBUTION_CANT_HAVE_MAPPER);
    }

    ArrayDistribution(const ArrayDistribution& other):
        _partitioningSchema(other._partitioningSchema),
        _distMapper(other._distMapper),
        _nodeId(other._nodeId)
    {
        if (_distMapper.get() != NULL && _partitioningSchema == psUndefined)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNDEFINED_DISTRIBUTION_CANT_HAVE_MAPPER);
    }

    virtual ~ArrayDistribution()
    {}

    ArrayDistribution& operator= (const ArrayDistribution& rhs)
    {
        if (this != &rhs)
        {
            _partitioningSchema = rhs._partitioningSchema;
            _distMapper = rhs._distMapper;
            _nodeId = rhs._nodeId;
        }
        return *this;
    }

    bool hasMapper() const
    {
        return _distMapper.get() != NULL;
    }

    bool isUndefined() const
    {
        return _partitioningSchema == psUndefined;
    }

    bool isViolated() const
    {
        return isUndefined() || hasMapper();
    }

    PartitioningSchema getPartitioningSchema() const
    {
        return _partitioningSchema;
    }

    boost::shared_ptr <DistributionMapper> getMapper() const
    {
        return _distMapper;
    }

    int64_t getNodeId() const
    {
        return _nodeId;
    }

    friend bool operator== (ArrayDistribution const& lhs, ArrayDistribution const& rhs);
    friend bool operator!= (ArrayDistribution const& lhs, ArrayDistribution const& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist);
};

class PhysicalBoundaries
{
private:
    Coordinates _startCoords;
    Coordinates _endCoords;
    double _density;

public:

    /**
     * Create a new set of boundaries assuming that the given schema is completely full of cells (fully dense array).
     * @param schema desired array shape
     * @return boundaries with coordinates at edges of schema
     */
    static PhysicalBoundaries createFromFullSchema(ArrayDesc const& schema );

    /**
     * Create a new set of boundaries that span numDimensions dimensions but contain 0 cells (fully sparse array).
     * @param numDimensions desired number of dimensions
     * @return boundaries with numDimensions nonintersecting coordinates.
     */
    static PhysicalBoundaries createEmpty(size_t numDimensions);

    /**
     * Create a new set of boundaries that span numDimensions dimensions but contain 0 cells (fully sparse array).
     * @return boundaries with numDimensions nonintersecting coordinates.
     */
    static uint64_t getCellNumber(Coordinates const & in, Dimensions const& dims);
    static uint64_t getCellsPerChunk (Dimensions const& dims);
    static uint32_t getCellSizeBytes(const Attributes& attrs);
    static Coordinates reshapeCoordinates (Coordinates const& in, Dimensions const& currentDims, Dimensions const& newDims);

    PhysicalBoundaries()
    {}
    PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density = 1.0);
    ~PhysicalBoundaries()
    {}

    const Coordinates & getStartCoords() const
    {
        return _startCoords;
    }

    const Coordinates & getEndCoords() const
    {
        return _endCoords;
    }

    double getDensity() const
    {
        return _density;
    }

    bool isEmpty() const;

    bool isInsideBox (Coordinate const& in, size_t const& dimensionNum) const;

    static uint64_t getNumCells (Coordinates const& start, Coordinates const& end);
    uint64_t getNumCells() const;
    uint64_t getRawCellNumber(Coordinates const & in) const;
    Coordinates coordsFromRawCellNumber (uint64_t cellNumber) const;

    uint64_t getNumChunks(Dimensions const& dims) const;
    double getSizeEstimateBytes(const ArrayDesc& schema) const;

    PhysicalBoundaries intersectWith (PhysicalBoundaries const& other) const;
    PhysicalBoundaries unionWith (PhysicalBoundaries const& other) const;
    PhysicalBoundaries crossWith (PhysicalBoundaries const& other) const;
    PhysicalBoundaries reshape(Dimensions const& oldDims, Dimensions const& newDims) const;

    shared_ptr<SharedBuffer> serialize() const;
    static PhysicalBoundaries deSerialize(shared_ptr<SharedBuffer> const& buf);

    friend std::ostream& operator<<(std::ostream& stream, const PhysicalBoundaries& bounds);
};

class DistributionRequirement
{
public:
    enum reqType
    {
        Any,
        Collocated,
        SpecificAnyOrder
    };

    DistributionRequirement (reqType rt = Any, vector<ArrayDistribution> specificRequirements = vector<ArrayDistribution>(0)):
        _requiredType(rt), _specificRequirements(specificRequirements)
    {
        if ((_requiredType == SpecificAnyOrder || _specificRequirements.size() != 0)
            && (_requiredType != SpecificAnyOrder || _specificRequirements.size() <= 0))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_SPECIFIC_DISTRIBUTION_REQUIRED);
        }
    }

    virtual ~DistributionRequirement()
    {}

    reqType getReqType() const
    {
        return _requiredType;
    }

    vector<ArrayDistribution> const& getSpecificRequirements()
    {
        return _specificRequirements;
    }

private:
    reqType _requiredType;
    vector<ArrayDistribution> _specificRequirements;
};

class DimensionGrouping
{
public:
    DimensionGrouping()
    {}

    DimensionGrouping(Dimensions const& originalDimensions,
                      Dimensions const& groupedDimensions) :
        _dimensionMask(0)
    {
        for (size_t i = 0; i < groupedDimensions.size(); i++)
        {
            DimensionDesc d = groupedDimensions[i];
            for (size_t j = 0; j < originalDimensions.size(); j++)
            {
                DimensionDesc dO = originalDimensions[j];
                if (dO.getBaseName() == d.getBaseName() &&
                    dO.getLength() == d.getLength())
                {
                    _dimensionMask.push_back(j);
                }
            }
        }
    }

    inline Coordinates reduceToGroup(Coordinates const& in) const
    {
        if (_dimensionMask.size()==0)
        {
            return Coordinates(1,0);
        }

        Coordinates out(_dimensionMask.size());
        for (size_t i = 0; i < _dimensionMask.size(); i++)
        {
            out[i] = in[_dimensionMask[i]];
        }
        return out;
    }

    inline void reduceToGroup(Coordinates const& in, Coordinates & out) const
    {
        if (_dimensionMask.size()==0)
        {
            out[0]=0;
            return;
        }

        for (size_t i = 0; i < _dimensionMask.size(); i++)
        {
            out[i] = in[_dimensionMask[i]];
        }
    }

private:
    vector<size_t> _dimensionMask;
};

/**
 *      This is the parent class for all physical operators. In order to add new physical operator
 *      you must inherit new class from this class and implement methods. Note, every physical
 *      operator has a logical and implement it. So inferring schema already should be done in
 *      logical operator.
 */
class PhysicalOperator
{
public:
        typedef boost::shared_ptr<OperatorParam> Parameter;

    typedef std::vector<boost::shared_ptr<OperatorParam> > Parameters;

    PhysicalOperator(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters,
            const ArrayDesc& schema):
    _parameters(parameters),  _schema(schema), _tileMode(false), _logicalName(logicalName), _physicalName(physicalName)
    {
    }

    virtual ~PhysicalOperator(){}

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    const std::string& getPhysicalName() const
    {
        return _physicalName;
    }

    const Parameters& getParameters() const
    {
            return _parameters;
    }

    const ArrayDesc& getSchema() const
    {
            return _schema;
    }

    void setSchema(const ArrayDesc& schema)
    {
        _schema = schema;
    }

    void setQuery(const boost::shared_ptr<Query>& query)
    {
       _query = query;
    }

    boost::shared_ptr<Query> getQuery()
    {
       return _query.lock();
    }

    /**
     * This method is executed on coordinator node before sending out plan on remote nodes and
     * before local call of execute method.
     */
    virtual void preSingleExecute(boost::shared_ptr<Query>)
    {
    }

    /**
     * This method is executed on coordinator node before sending out plan on remote nodes and
     * after call of execute method in overall cluster.
     */
    virtual void postSingleExecute(boost::shared_ptr<Query>)
    {
    }

    Statistics& getStatistics() {
        return _statistics;
    }

        void setParameters(const Parameters& parameters)
        {
                _parameters = parameters;
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
            //Could also call toString on the operator but gets to be too much clutter

            for ( int i = 0; i < indent; i++)
            {
                    str<<" ";
            }
            str<<"schema "<<_schema<<"\n";
    }

    virtual boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >&,  boost::shared_ptr<Query>) = 0;

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Any);
    }

    /**
     *  [Optimizer API] Determine if the output schema of this
     *  operator will be distributed correctly - exactly as specified by
     *  getSchema(). Optimizer may insert SG operations for operators that
     *  do not preserve distribution.
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return true if output distribution is guraranteed correct. false otherwise.
     */
    virtual bool isDistributionPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return true;
    }

    /**
     *  [Optimizer API] Determine if the output chunks of this operator
     *  will be well formed. Optimizer may insert SG operations for operators
     *  taht do not preserve chunking.
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return true if output chunking is guraranteed correct. false otherwise.
     */
    virtual bool isChunkPreserving(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return true;
    }

    /**
     *  [Optimizer API] Determine the distribution of operator output.
     *  @param inputDistributions distributions of inputs that will be provided in order same as inputSchemas
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return distribution of the output
     */
    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        //if you override isDistributionPreserving, you MUST override getOutputDistribution
        assert(isDistributionPreserving(inputSchemas));

        if(inputDistributions.size())
        {
            return inputDistributions[0];
        }
        return ArrayDistribution(psRoundRobin);
    }

    /**
     *  [Optimizer API] Determine the boundaries of operator output.
     *  @param inputBoundaries the boundaries of inputs that will be provided in order same as inputSchemas
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return boundaries of the output
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return PhysicalBoundaries::createFromFullSchema(_schema);
    }

    /**
     *  [Optimizer API] Determine if the operator requires a repart node.
     *  @param inputSchema shape of array that will given as input (op must be unary)
     *  @return true if repart is needed. If so, getRepartSchema() will provide the schema of needed repart.
     */
    virtual bool requiresRepart(ArrayDesc const& inputSchema) const
    {
        return false;
    }

    /**
     *  [Optimizer API] Determine the repart schema required
     *  @param inputSchema shape of array that will given as input (op must be unary)
     *  @return the required schema of input (inputSchema with adjusted chunk sizes and overlap sizes)
     */
    virtual ArrayDesc getRepartSchema(ArrayDesc const& inputSchema) const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNEXPECTED_GETREPARTSCHEMA);
    }

    bool getTileMode() const
    {
        return _tileMode;
    }

    void setTileMode(bool enabled) 
    { 
        _tileMode = enabled;
    }

protected:
    Parameters _parameters;
    ArrayDesc _schema;
    Statistics _statistics;
    bool _tileMode;

    boost::weak_ptr<Query> _query;

private:
    std::string _logicalName;
    std::string _physicalName;
};


/**
 * It's base class for constructing logical operators. It declares some virtual functions that
 * will have template implementation.
 */
class BaseLogicalOperatorFactory
{
public:
    BaseLogicalOperatorFactory(const std::string& logicalName): _logicalName(logicalName) {
    }

    virtual ~BaseLogicalOperatorFactory() {}

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    virtual boost::shared_ptr<LogicalOperator> createLogicalOperator(const std::string& alias) = 0;

  protected:
    void registerFactory();

    std::string _logicalName;
};

/**
 * This is template implementation of logical operators factory. To declare operator factory for
 * new logical operator just declare variable
 * LogicalOperatorFactory<NewLogicalOperator> newLogicalOperatorFactory("logical_name");
 */
template<class T>
class LogicalOperatorFactory: public BaseLogicalOperatorFactory
{
  public:
    LogicalOperatorFactory(const std::string& logicalName): BaseLogicalOperatorFactory(logicalName) {
    }

    virtual ~LogicalOperatorFactory() {}

    boost::shared_ptr<LogicalOperator> createLogicalOperator(const std::string& alias)
    {
        return boost::shared_ptr<LogicalOperator>(new T(_logicalName, alias));
    }
};


template<class T>
class UserDefinedLogicalOperatorFactory: public LogicalOperatorFactory<T>
{
  public:
    UserDefinedLogicalOperatorFactory(const std::string& logicalName): LogicalOperatorFactory<T>(logicalName) {
        BaseLogicalOperatorFactory::registerFactory();
    }
    virtual ~UserDefinedLogicalOperatorFactory() {}
};

/**
 * It's base class for constructing physical operators. It declares some virtual functions that
 * will have template implementation.
 */
class BasePhysicalOperatorFactory
{
  public:
    BasePhysicalOperatorFactory(const std::string& logicalName, const std::string& physicalName):
        _logicalName(logicalName), _physicalName(physicalName) {
    }
    virtual ~BasePhysicalOperatorFactory() {}

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    const std::string& getPhysicalName() const
    {
        return _physicalName;
    }

    virtual boost::shared_ptr<PhysicalOperator> createPhysicalOperator(const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema) = 0;

protected:
    void registerFactory();

    std::string _logicalName;
    std::string _physicalName;
};

/**
 * This is template implementation of physical operators factory. To declare operator factory for
 * new physical operator just declare variable
 * PhysicalOperatorFactory<NewPhysicalOperator> newPhysicalOperatorFactory("logical_name", "physical_name");
 */
template<class T>
class PhysicalOperatorFactory: public BasePhysicalOperatorFactory
{
  public:
    PhysicalOperatorFactory(const std::string& logicalName, const std::string& physicalName):
        BasePhysicalOperatorFactory(logicalName, physicalName) {
    }
    virtual ~PhysicalOperatorFactory() {}

    boost::shared_ptr<PhysicalOperator> createPhysicalOperator(const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema)
    {
        return boost::shared_ptr<PhysicalOperator>(new T(_logicalName, _physicalName, parameters, schema));
    }
};

template<class T>
class UserDefinedPhysicalOperatorFactory: public PhysicalOperatorFactory<T>
{
  public:
    UserDefinedPhysicalOperatorFactory(const std::string& logicalName, const std::string& physicalName)
    : PhysicalOperatorFactory<T>(logicalName, physicalName)
    {
        BasePhysicalOperatorFactory::registerFactory();
    }
    virtual ~UserDefinedPhysicalOperatorFactory() {}
};

#define DECLARE_LOGICAL_OPERATOR_FACTORY(name, uname) static LogicalOperatorFactory<name> _logicalFactory##name(uname); \
        BaseLogicalOperatorFactory* get_logicalFactory##name() { return &_logicalFactory##name; }

#define DECLARE_PHYSICAL_OPERATOR_FACTORY(name, ulname, upname) static PhysicalOperatorFactory<name> _physicalFactory##name(ulname, upname); \
        BasePhysicalOperatorFactory* get_physicalFactory##name() { return &_physicalFactory##name; }

#define REGISTER_LOGICAL_OPERATOR_FACTORY(name, uname) static UserDefinedLogicalOperatorFactory<name> _logicalFactory##name(uname)

#define REGISTER_PHYSICAL_OPERATOR_FACTORY(name, ulname, upname) static UserDefinedPhysicalOperatorFactory<name> _physicalFactory##name(ulname, upname)


NodeID getNodeForChunk(boost::shared_ptr<Query> query,
                       Coordinates chunkPosition,
                       ArrayDesc const& desc,
                       PartitioningSchema ps,
                       boost::shared_ptr<DistributionMapper> distMapper,
                       NodeID defaultNodeId);


/**
 * This function perform repartitioning of inputArray.
 *
 * @param inputArray a pointer to local part of repartitioning array
 * @param query a query context
 * @param ps a new partitioning schema
 * @param resultArrayName a name of array where to store result of repartitioning.
 *      This array must exist.
 * @return pointer to new local array with part of array after repart.
 */
boost::shared_ptr< Array> redistribute(boost::shared_ptr< Array> inputArray, boost::shared_ptr< Query> query,
                                              PartitioningSchema ps,
                                              const std::string& resultArrayName = "",
                                              NodeID nodeID = NodeID(~0),
                                              boost::shared_ptr<DistributionMapper> distMapper = boost::shared_ptr<DistributionMapper> ());

AggregatePtr resolveAggregate(boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                              Attributes const& inputAttributes,
                              AttributeID* inputAttributeID = NULL,
                              string* outputName = NULL);

void addAggregatedAttribute (boost::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                             ArrayDesc const& inputDesc,
                             ArrayDesc& outputDesc);


/**
 * Redistribute inputArray using the psRoundRobin distribution scheme, using aggregate merge to merge overlapping cells.
 *
 * @param inputArray a pointer to local part of repartitioning array
 * @param query a query context
 * @param aggs a list of aggregates to use. Must have as many pointers as there are attributes in the array. If any pointers are NULL,
 *      the corresponding attribute is not aggregated (non-deterministic result in case of aggregate).
 * @return pointer to new local array with part of array after repart.
 */
boost::shared_ptr<MemArray> redistributeAggregate(boost::shared_ptr<MemArray> inputArray,
                                                  boost::shared_ptr<Query> query,
                                                  vector <AggregatePtr> const& aggs);

PhysicalBoundaries findArrayBoundaries(shared_ptr<Array> srcArray,
                                       boost::shared_ptr<Query> query,
                                       bool global = true);


/**
 * Build functional attribute mapping
 * @param dim dimension for which mapping should be build
 * @return AttributeMap providing required mapping
 */
boost::shared_ptr<AttributeMap> buildFunctionalMapping(DimensionDesc const& dim);

/**
 * Build a sorted index from the given attribute set.
 * In order to save memory footprint, the given attrSet is sorted in-place; therefore it is mutated.
 * @param attrSet the attributes to index
 * @param query for reference
 * @param indexArrayName the name under which the index will be stored. Index is not stored if empty.
 * @param indexStart the desired minimum coordinate of index array and index position of first element in index
 * @param maxElements maximum index size. If set, throw exception if index size exceeds this value.
 * @return AttributeMap containing the entire index.
 */
boost::shared_ptr<AttributeMap> buildSortedIndex( AttributeSet& attrSet,
                                                  boost::shared_ptr<Query> query,
                                                  string const& indexArrayName = "",
                                                  Coordinate indexStart = 0,
                                                  uint64_t maxElements = 0);

/**
 * Build a sorted index based on an attribute of the given array, persist if necessary.
 * @param srcArray a pointer to local part of array
 * @param attrID the attribute to index
 * @param query a query context
 * @param indexArrayName the name under which the index will be stored. Index is not stored if empty.
 * @param indexStart the desired minimum coordinate of index array and index position of first element in index
 * @param maxElements maximum index size. If set, throw exception if index size exceeds this value.
 * @return AttributeMap containing the entire index.
 */
boost::shared_ptr<AttributeMap> buildSortedIndex( shared_ptr<Array> srcArray,
                                                  AttributeID attrID,
                                                  boost::shared_ptr<Query> query,
                                                  string const& indexArrayName = "",
                                                  Coordinate indexStart = 0,
                                                  uint64_t maxElements = 0);

} // namespace


#endif /* OPERATOR_H_ */
