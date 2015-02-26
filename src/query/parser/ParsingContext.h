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
 * @file
 *
 * @brief Class which holding query string and references to it for jamming through
 * AST and logical query tree for providing contextual errors.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef PARSINGCONTEXT_H_
#define PARSINGCONTEXT_H_

#include <string>
#include <stdint.h>
#include <boost/shared_ptr.hpp>

namespace scidb
{

class ParsingContext
{
public:
    ParsingContext(const std::string& queryString, uint32_t lineStart = 0, uint32_t colStart = 0, uint32_t lineEnd = 0, uint32_t colEnd = 0) :
        _lineStart(lineStart),
        _colStart(colStart),
        _lineEnd(lineEnd),
        _colEnd(colEnd)
    {
        _queryString = boost::shared_ptr<std::string>(new std::string(queryString));
    }

    ParsingContext(boost::shared_ptr<ParsingContext> context, uint32_t lineStart, uint32_t colStart, uint32_t lineEnd, uint32_t colEnd) :
        _queryString(context->_queryString),
        _lineStart(lineStart),
        _colStart(colStart),
        _lineEnd(lineEnd),
        _colEnd(colEnd)
    {
    }

    const std::string& getQueryString() const
    {
        return *_queryString;
    }

    uint32_t getLineStart() const
    {
        return _lineStart;
    }

    uint32_t getLineEnd() const
    {
        return _lineEnd;
    }

    uint32_t getColStart() const
    {
        return _colStart;
    }

    uint32_t getColEnd() const
    {
        return _colEnd;
    }

private:
    boost::shared_ptr<std::string> _queryString;

    uint32_t _lineStart;

    uint32_t _colStart;

    uint32_t _lineEnd;

    uint32_t _colEnd;
};

}

#endif /* PARSINGCONTEXT_H_ */
