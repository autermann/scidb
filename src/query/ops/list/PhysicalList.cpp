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
 * @file PhysicalExample.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of LIST operator for listing data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "system/SystemCatalog.h"
#include "query/UDT.h"
#include "query/TypeSystem.h"
#include "util/PluginManager.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalList: public PhysicalOperator
{
public:
    PhysicalList(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psLocalNode);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        vector<string> list;
        string what;
        bool showSystem = false;
        if (_parameters.size() >= 1)
        {
            what = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
        }
        else
        {
            what = "arrays";
        }

        if (_parameters.size() == 2)
        {
            showSystem = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getBool();
        }

        if (what == "aggregates") {
            list = AggregateLibrary::getInstance()->getAggregateNames();
        } else if (what == "arrays") {
            SystemCatalog::getInstance()->getArrays(list);
            if (!showSystem)
            {
                vector<string>::iterator it = list.begin();
                while(it != list.end())
                {
                    if (it->find('@') != string::npos || it->find(':') != string::npos)
                    {
                        it = list.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
        } else if (what == "operators") { 
            OperatorLibrary::getInstance()->getLogicalNames(list);
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(2);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setData(list[i].c_str(), list[i].length() + 1);
                const string& libraryName = OperatorLibrary::getInstance()->getOperatorLibraries().getObjectLibrary(list[i]);
                tuple[1].setString(libraryName.c_str());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "types") {
#if 0
            printf("Matrix<double>(10,10):\n");
            Matrix<double> m(10, 10);
            someGenericAlgorithm<Matrix<double>, double>(m);
            printf("MatrixOfUDT(10,10):\n");
            MatrixOfUDT mUDT(TID_DOUBLE, 10, 10);
            someGenericAlgorithm<MatrixOfUDT, UDT::Val>(mUDT);
#endif
            list = TypeLibrary::typeIds();
            vector< boost::shared_ptr<Tuple> > tuples(list.size());
            for (size_t i = 0; i < tuples.size(); i++) {
                Tuple& tuple = *new Tuple(2);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
                tuple[0].setData(list[i].c_str(), list[i].length() + 1);
                const string& libraryName = TypeLibrary::getTypeLibraries().getObjectLibrary(list[i]);
                tuple[1].setString(libraryName.c_str());
            }
            return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "functions") {
            vector<boost::shared_ptr<Tuple> > tuples;
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin();
                 i != funcs.end(); ++i)
            {
                for (funcDescTypesMap::const_iterator j = i->second.begin();
                     j != i->second.end(); ++j)
                {
                    Tuple& tuple = *new Tuple(4);
                    FunctionDescription const& func = j->second;
                    tuples.push_back(shared_ptr<Tuple>(&tuple));
                    tuple[0].setString(func.getName().c_str());
                    const string mangleName = func.getMangleName();
                    tuple[1].setString(mangleName.c_str());
                    tuple[2].setBool(func.isDeterministic());
                    const string& libraryName = FunctionLibrary::getInstance()->getFunctionLibraries().getObjectLibrary(mangleName);
                    tuple[3].setString(libraryName.c_str());
                }
            }
            Tuple& tuple1 = *new Tuple(4);
            tuples.push_back(shared_ptr<Tuple>(&tuple1));
            tuple1[0].setString("iif");
            tuple1[1].setString("<any> iif(bool, <any>, <any>)");
            tuple1[2].setBool(true);
            tuple1[3].setString("scidb");
            Tuple& tuple2 = *new Tuple(4);
            tuples.push_back(shared_ptr<Tuple>(&tuple2));
            tuple2[0].setString("missing_reason");
            tuple2[1].setString("int32 missing_reason(<any>)");
            tuple2[2].setBool(true);
            tuple2[3].setString("scidb");
            return shared_ptr<Array>(new TupleArray(_schema, tuples));
        } else if (what == "libraries") {
            vector<boost::shared_ptr<Tuple> > tuples;
            const std::map<std::string, PluginDesc>& plugins = PluginManager::getInstance()->getPlugins();
            for (std::map<std::string, PluginDesc>::const_iterator i = plugins.begin();
                 i != plugins.end(); ++i)
            {
                Tuple& tuple = *new Tuple(5);
                PluginDesc const& pluginDesc = i->second;
                tuples.push_back(shared_ptr<Tuple>(&tuple));
                tuple[0].setString(i->first.c_str());
                tuple[1].setInt32(pluginDesc.major);
                tuple[2].setInt32(pluginDesc.minor);
                tuple[3].setInt32(pluginDesc.patch);
                tuple[4].setInt32(pluginDesc.build);
            }
            return shared_ptr<Array>(new TupleArray(_schema, tuples));
         } else if (what == "queries") {
            vector<boost::shared_ptr<Tuple> > tuples;
            ScopedMutexLock scope(Query::queriesMutex);
            for (map<QueryID, shared_ptr<Query> >::const_iterator i = Query::getQueries().begin();
                 i != Query::getQueries().end(); ++i)
            {
                const Query& query = *i->second;
                Tuple& tuple = *new Tuple(6);
                tuples.push_back(shared_ptr<Tuple>(&tuple));
                tuple[0].setString(query.queryString.c_str());
                tuple[1].setInt64(i->first);
                tuple[2].setDateTime(query.getCreationTime());
                tuple[3].setInt32(query.getErrorCode());
                tuple[4].setString(query.getErrorDescription().c_str());
                tuple[5].setBool(query.idle());
            }
            return shared_ptr<Array>(new TupleArray(_schema, tuples));
         } else if (what == "nodes") {
           return listNodes(query);
        } else {
           assert(0);
        }
        vector< boost::shared_ptr<Tuple> > tuples(list.size());
        for (size_t i = 0; i < tuples.size(); i++) { 
            Tuple& tuple = *new Tuple(1);
            tuples[i] = boost::shared_ptr<Tuple>(&tuple);
            tuple[0].setData(list[i].c_str(), list[i].length() + 1);
        } 
        return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
    }

   boost::shared_ptr<Array> listNodes(const boost::shared_ptr<Query>& query)
   {
      boost::shared_ptr<const NodeLiveness> queryLiveness(query->getCoordinatorLiveness());
      Nodes nodes;
      SystemCatalog::getInstance()->getNodes(nodes);

      assert(queryLiveness->getNumNodes() == nodes.size());

      vector<boost::shared_ptr<Tuple> > tuples;
      tuples.reserve(nodes.size());
      for (Nodes::const_iterator iter = nodes.begin();
           iter != nodes.end(); ++iter) {

         shared_ptr<Tuple> tuplePtr(new Tuple(4));
         Tuple& tuple = *tuplePtr.get();
         tuples.push_back(tuplePtr);

         const NodeDesc& nodeDesc = *iter;
         NodeID nodeId = nodeDesc.getNodeId();
         time_t t = static_cast<time_t>(nodeDesc.getOnlineSince());
         tuple[0].setString(nodeDesc.getHost().c_str());
         tuple[1].setUint16(nodeDesc.getPort());
         tuple[2].setUint64(nodeId);
         if ((t == (time_t)0) || queryLiveness->isDead(nodeId)){
            tuple[3].setString("offline");
         } else {
            assert(queryLiveness->find(nodeId));
            struct tm date;

            if (!(&date == gmtime_r(&t, &date)))
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_GENERATE_UTC_TIME);

            string out(boost::str(boost::format("%04d-%02d-%02d %02d:%02d:%02d")
                                  % (date.tm_year+1900)
                                  % (date.tm_mon+1)
                                  % date.tm_mday
                                  % date.tm_hour
                                  % date.tm_min
                                  % date.tm_sec));
            tuple[3].setString(out.c_str());
         }
      }
      return shared_ptr<Array>(new TupleArray(_schema, tuples));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalList, "list", "physicalList")

} //namespace
