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
 * @brief Parsing and translation AQL/AFL AST tree into AQL query tree.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author Pavel Velikhov <pavel.velikhov@gmail.com>
 */

#ifndef ALTRANSLATOR_H_
#define ALTRANSLATOR_H_

namespace boost
{
    template<class T> class shared_ptr;
}

namespace scidb
{

/**
 * Check syntax and semantic of AFL/AQL AST and convert it to appropriate logical plan nodes
 * 
 * @param ast top node of AST
 * @param query 
 */
boost::shared_ptr<class LogicalQueryPlanNode> AstToLogicalPlan(
        class AstNode *ast,
        const boost::shared_ptr<class Query> &query);

boost::shared_ptr<class LogicalExpression> AstToLogicalExpression(class AstNode *ast);

}
#endif /* ALTRANSLATOR_H_ */
