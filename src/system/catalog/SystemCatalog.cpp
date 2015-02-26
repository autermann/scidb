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

/*!
 *      @file
 *
 *      @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 *      @brief API for fetching and updating system catalog metadata.
 */

#include <vector>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iostream>

#include <boost/random.hpp>

#include <pqxx/connection>
#include <pqxx/transaction>
#include <pqxx/prepared_statement>
#include <pqxx/except>
#include <pqxx/binarystring>
#include <libpq-fe.h>

#include "log4cxx/logger.h"

#include "system/Config.h"
#include "smgr/io/Storage.h"
#include "system/Exceptions.h"
#include "system/ErrorCodes.h"
#include "util/Mutex.h"

#include "system/SystemCatalog.h"
#include "system/SciDBConfigOptions.h"
#include "system/ErrorCodes.h"

#include "query/Expression.h"
#include "query/parser/Serialize.h"

using namespace std;
using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb
{

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.catalog"));

    static const int WAIT_LOCK_TIMEOUT = 5; // 5 seconds

    boost::mt19937 _rng;
    Mutex SystemCatalog::_pgLock;

    std::string getExeDir()
    {
        char exepath[1024] = {0};
        readlink("/proc/self/exe", exepath, sizeof(exepath));
        for (size_t i = strlen(exepath) - 1; i !=0; i--) {
            if (exepath[i] == '/') {
                exepath[i] = 0;
                break;
            }
        }
        return exepath;
    }


    const std::string& SystemCatalog::initializeCluster()
    {
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::initializeCluster()");

        try
        {
            work tr(*_connection);
            const string metaSql = Config::getInstance()->getOption<string>(CONFIG_METADATA);
            ifstream ifs(metaSql.c_str(), ios::in);
            if(!ifs.is_open())
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_OPEN_FILE) << metaSql << errno;
            string sql((istreambuf_iterator<char>(ifs)), istreambuf_iterator<char>());
            ifs.close();
            tr.exec(sql);

            result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
            _uuid = query_res[0].at("uuid").as(string());
            _initialized = true;

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when initializing cluster";
        }

        LOG4CXX_TRACE(logger, "Initialized cluster uuid = " << _uuid);

        return _uuid;
    }

    bool SystemCatalog::isInitialized() const
    {
        return _initialized;
    }

    const std::string& SystemCatalog::getClusterUuid() const
    {
        return _uuid;
    }

    ArrayID SystemCatalog::addArray(const ArrayDesc &array_desc, PartitioningSchema ps)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addArray ( array_name = " << array_desc.getName() << ")");
        LOG4CXX_TRACE(logger, "New Array    = " << array_desc );
        LOG4CXX_TRACE(logger, "Partitioning = " << ps );

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        ArrayID array_id;

        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('array_id_seq')");
            array_id = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"array\"(id, name, partitioning_schema, flags, comment) values ($1, $2, $3, $4, $5)";
            _connection->prepare(sql1, sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string)
                ("integer", treat_direct)
                ("integer", treat_direct)
                ("varchar", treat_string);
            tr.prepared(sql1)
                (array_id)
                (array_desc.getName())
                ((int) ps)
                (array_desc.getFlags())
                (array_desc.getComment()).exec();

            string sql2 = "insert into \"array_attribute\"(array_id, id, name, type, flags, "
                " default_compression_method, reserve, default_missing_reason, default_value, comment) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
            _connection->prepare(sql2, sql2)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("varchar", treat_string);

            Attributes const& attributes = array_desc.getAttributes();
            Attributes cachedAttributes(attributes.size());
            for (int i = 0, n = attributes.size(); i < n; i++)
            {
                AttributeDesc const& attr = attributes[i];
                tr.prepared(sql2)
                    (array_id)
                    (i)
                    (attr.getName())
                    (attr.getType())
                    (attr.getFlags())
                    (attr.getDefaultCompressionMethod())
                    (attr.getReserve())
                    (attr.getDefaultValue().getMissingReason())
                    (attr.getDefaultValueExpr())
                    (attr.getComment()).exec();

                //Attribute in descriptor has no some data before adding to catalog so build it manually for
                //caching
                cachedAttributes[i] =
                    AttributeDesc(i, attr.getName(), attr.getType(), attr.getFlags(),
                                  attr.getDefaultCompressionMethod(), std::set<std::string>(), attr.getReserve(),
                                  &attr.getDefaultValue(), attr.getDefaultValueExpr(), attr.getComment());
            }

            //
            // string sql3 = "insert into \"array_dimension\"(array_id, id, name,"
            //      " start, length, chunk_interval, chunk_overlap) values ($1, $2, $3, $4, $5, $6, $7)";
            //
            string sql3 = "insert into \"array_dimension\"(array_id, id, name,"
                " startMin, currStart, currEnd, endMax, chunk_interval, chunk_overlap, type, source_array_name, comment) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
            _connection->prepare(sql3, sql3)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("varchar", treat_string)
                ("varchar", treat_string)
                ;

            Dimensions const& dims = array_desc.getDimensions();
            for (int i = 0, n = dims.size(); i < n; i++)
            {
                DimensionDesc const& dim = dims[i];
                tr.prepared(sql3)
                    (array_id)
                    (i)
                    (dim.getBaseName())
                    (dim.getStartMin())
                    (dim.getCurrStart())
                    (dim.getCurrEnd())
                    (dim.getEndMax())
                    (dim.getChunkInterval())
                    (dim.getChunkOverlap())
                    (dim.getType())
                    (dim.getSourceArrayName())
                    (dim.getComment()).exec();
                //TODO: If DimensionDesc will store IDs in future, here must be building vector of
                //dimensions for caching as for attributes.
            }

            tr.commit();

            //Caching descriptor builded manually because passed in arguments has no right IDs
            boost::shared_ptr<ArrayDesc> cachedArrayDesc(new ArrayDesc(array_id,
                                                                             array_desc.getName(),
                                                                             cachedAttributes,
                                                                             array_desc.getDimensions(),
                                                                             array_desc.getFlags(),
                                                                             array_desc.getComment()));

            arrayDescByID[array_id] = cachedArrayDesc;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when adding array";
        }

        return array_id;
    }

    void SystemCatalog::updateArray(const ArrayDesc &array_desc)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::updateArray ( old_array_ID = " << array_desc.getId() << ", new array_name = " << array_desc.getName() << ")");

        ScopedMutexLock mutexLock(_pgLock);
        const boost::shared_ptr<ArrayDesc>& oldArrayDesc = getArrayDesc(array_desc.getId());

        LOG4CXX_TRACE(logger, "Previously = " << *oldArrayDesc);
        LOG4CXX_TRACE(logger, "New        = " << array_desc);

        assert(_connection);
        ArrayID array_id = array_desc.getId();

        try
        {
            work tr(*_connection);

            if (array_desc.getName() != oldArrayDesc->getName()) {
                _connection->prepare("updateArray", "update \"array\" set name = t.new_name from\
                    (select name || '_' || version_id as old_name, $1 || '_' || version_id as new_name from \"array\" join array_version on array_id = id where id = $2) as t\
                    where \"array\".name = t.old_name")
                    ("varchar", treat_string)
                    ("bigint", treat_direct);
                tr.prepared("updateArray")
                    (array_desc.getName())
                    (array_id).exec();
                _connection->unprepare("updateArray");

                string sql1 = "update \"array\" set name=$2, flags=$3, comment=$4 where id=$1";
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("varchar", treat_string)
                    ("integer", treat_direct)
                    ("varchar", treat_string);
                tr.prepared(sql1)
                    (array_id)
                    (array_desc.getName())
                    (array_desc.getFlags())
                    (array_desc.getComment()).exec();
            } else {
                string sql1 = "update \"array\" set flags=$2, comment=$3 where id=$1";
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("varchar", treat_string);
                tr.prepared(sql1)
                    (array_id)
                    (array_desc.getFlags())
                    (array_desc.getComment()).exec();
            }

            string sql2 = "update \"array_attribute\" set name=$3, type=$4, flags=$5, default_compression_method=$6, reserve=$7, default_missing_reason=$8, default_value=$9, comment=$10 where array_id=$1 and id=$2";
            _connection->prepare(sql2, sql2)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("varchar", treat_string);

            Attributes const& attributes = array_desc.getAttributes();
            for (int i = 0, n = attributes.size(); i < n; i++)
            {
                AttributeDesc const& attr = attributes[i];
                tr.prepared(sql2)
                    (array_id)
                    (i)
                    (attr.getName())
                    (attr.getType())
                    (attr.getFlags())
                    (attr.getDefaultCompressionMethod())
                    (attr.getReserve())
                    (attr.getDefaultValue().getMissingReason())
                    (attr.getDefaultValueExpr())
                    (attr.getComment()).exec();
            }

            string sql3 = "update \"array_dimension\" set name=$3, startMin=$4, endMax=$5, chunk_interval=$6, chunk_overlap=$7, type=$8, source_array_name=$9, comment=$10 where array_id=$1 and id=$2";

            _connection->prepare(sql3, sql3)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct)
                ("int", treat_direct)
                ("varchar", treat_string)
                ("varchar", treat_string)
                ("varchar", treat_string);

            Dimensions const& dims = array_desc.getDimensions();
            for (int i = 0, n = dims.size(); i < n; i++)
            {
                DimensionDesc const& dim = dims[i];
                tr.prepared(sql3)
                    (array_id)
                    (i)
                    (dim.getBaseName())
                    (dim.getStartMin())
                    (dim.getEndMax())
                    (dim.getChunkInterval())
                    (dim.getChunkOverlap())
                    (dim.getType())
                    (dim.getSourceArrayName())
                    (dim.getComment()).exec();
            }

            tr.commit();

            boost::shared_ptr<ArrayDesc>& elem = arrayDescByID[array_desc.getId()];
            if (!elem) {
                const boost::shared_ptr<ArrayDesc> ad(new ArrayDesc(array_desc));
                elem = ad;
            } else {
                *elem = array_desc;
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when adding array";
        }
    }

    void SystemCatalog::getArrays(std::vector<std::string> &arrays) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrays()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            result query_res = tr.exec("select name from \"array\" where name is not null order by name");

            arrays.clear();
            arrays.reserve(query_res.size());

            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                arrays.push_back(i.at("name").c_str());
            }

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting array list";
        }

        LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");

    }

    bool SystemCatalog::containsArray(const ArrayID array_id) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::containsArray( id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        if (arrayDescByID.find(array_id) != arrayDescByID.end()) {
            return true;
        }

        LOG4CXX_TRACE(logger, "Failed to find array_id = " << array_id << " locally.");
        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id from \"array\" where id = $1";
            _connection->prepare(sql1, sql1)("bigint", treat_direct);
            result query_res1 = tr.prepared(sql1)(array_id).exec();
            return query_res1.size() != 0;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting array";
        }

        return false;
    }

    bool SystemCatalog::containsArray(const string &array_name) const
    {
        return findArrayByName(array_name) != INVALID_ARRAY_ID;
    }
    

    ArrayID SystemCatalog::findArrayByName(const std::string &array_name) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::containsArray( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock);

        LOG4CXX_TRACE(logger, "Failed to find array_name = " << array_name << " locally.");

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id from \"array\" where name = $1";
            _connection->prepare(sql1, sql1)("varchar", treat_string);
            result query_res1 = tr.prepared(sql1)(array_name).exec();
            if (query_res1.size() != 0) { 
                return query_res1[0].at("id").as(int64_t());
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting array";
        }
        return INVALID_ARRAY_ID;
    }

    bool SystemCatalog::getArrayDesc(const string &array_name, VersionID version, ArrayDesc &array_desc, const bool throwException)
    {
        if (getArrayDesc(array_name, array_desc, throwException)) {
            if (!array_desc.isImmutable()) {
                std::stringstream ss;
                if (version == LAST_VERSION) {
                    version = getLastVersion(array_desc.getId());
                    if (version == 0) {
                        return true;
                    }
                }
                ss << array_name << "@" << version;
                return getArrayDesc(ss.str(), array_desc, throwException);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    bool SystemCatalog::getArrayDesc(const string &array_name, ArrayDesc &array_desc, const bool throwException)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock);

        LOG4CXX_TRACE(logger, "Failed to find array_name = " << array_name << " locally.");
        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id, name, flags, comment from \"array\" where name = $1";
            _connection->prepare("find-by-name", sql1)("varchar", treat_string);
            result query_res1 = tr.prepared("find-by-name")(array_name).exec();

            if (query_res1.size() <= 0)
            {
                if (throwException) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << array_name;
                }
                return false;
            }
            ArrayID array_id = query_res1[0].at("id").as(int64_t());

            string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value, comment "
                " from \"array_attribute\" where array_id = $1 order by id";
            _connection->prepare(sql2, sql2)("integer", treat_direct);
            result query_res2 = tr.prepared(sql2)(array_id).exec();

            Attributes attributes;
            if (query_res2.size() > 0)
            {
                attributes.reserve(query_res2.size());
                for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
                {
                    Value defaultValue;
                    int missingReason = i.at("default_missing_reason").as(int());
                    string defaultValueExpr;
                    if (missingReason >= 0) {
                        defaultValue.setNull(missingReason);
                    } else {
                        defaultValueExpr = i.at("default_value").as(string());

                        // Evaluate expression if present and set default value
                        if (defaultValueExpr != "")
                        {
                            Expression expr = deserializePhysicalExpression(defaultValueExpr);
                            defaultValue = expr.evaluate();
                        }
                        // Else fallback to null or zero as before
                        else
                        {
                            defaultValue = Value(TypeLibrary::getType(i.at("type").as(TypeId())));
                            if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                            {
                                defaultValue.setNull();
                            }
                            else
                            {
                                defaultValue.setZero();
                            }
                        }
                    }
                    AttributeDesc att(
                        i.at("id").as(AttributeID()),
                        i.at("name").as(string()),
                        i.at("type").as(TypeId()),
                        i.at("flags").as(int16_t()),
                        i.at("default_compression_method").as(uint16_t()),
                        std::set<std::string>(),
                        i.at("reserve").as(int16_t()),
                        &defaultValue,
                        defaultValueExpr,
                        i.at("comment").as(string())
                        );
                    attributes.push_back(att);
                }
            }

            // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
            //          " from \"array_dimension\" where array_id = $1 order by id";
            string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap, type, source_array_name, comment "
                " from \"array_dimension\" where array_id = $1 order by id";

            _connection->prepare(sql3, sql3)("integer", treat_direct);
            result query_res3 = tr.prepared(sql3)(array_id).exec();

            Dimensions dimensions;
            if (query_res3.size() > 0)
            {
                attributes.reserve(query_res3.size());
                for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
                {
                    dimensions.push_back(
                        DimensionDesc(
                            i.at("name").as(string()),
                            i.at("startmin").as(int64_t()),
                            i.at("currstart").as(int64_t()),
                            i.at("currend").as(int64_t()),
                            i.at("endmax").as(int64_t()),
                            i.at("chunk_interval").as(size_t()),
                            i.at("chunk_overlap").as(size_t()),
                            i.at("type").as(string()),
                            i.at("source_array_name").as(string()),
                            i.at("comment").as(string())
                            ));
                }
            }

            boost::shared_ptr<ArrayDesc> ad(new ArrayDesc(array_id,
                                                                query_res1[0].at("name").as(string()),
                                                                attributes,
                                                                dimensions,
                                                                query_res1[0].at("flags").as(int()),
                                                                query_res1[0].at("comment").as(string())));

            array_desc = *ad;
            tr.commit();

            boost::shared_ptr<ArrayDesc>& elem = arrayDescByID[array_id];
            if (!elem) {
                elem = ad;
            } else {
                *elem = *ad;
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting array";
        }
        return true;
    }

    void SystemCatalog::getArrayDesc(const ArrayID array_id, ArrayDesc &array_desc)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ", array_desc )");
        boost::shared_ptr<ArrayDesc> ad = getArrayDesc(array_id);
        array_desc = *ad.get();
    }

    boost::shared_ptr<ArrayDesc> SystemCatalog::getArrayDesc(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        std::map<ArrayID, boost::shared_ptr<ArrayDesc> >::iterator i = arrayDescByID.find(array_id);
        if (i != arrayDescByID.end())
        {
            return i->second;
        }
        return reloadArrayDesc(array_id, true);
    }

    boost::shared_ptr<ArrayDesc> SystemCatalog::reloadArrayDesc(const ArrayID array_id, bool throwException)
    {
        LOG4CXX_TRACE(logger, "Failed to find array_id = " << array_id << " locally.");
        assert(_connection);
        boost::shared_ptr<ArrayDesc> array_desc;
        try
        {
            work tr(*_connection);
            string sql1 = "select id, name, flags, comment from \"array\" where id = $1";
            _connection->prepare("find-by-id", sql1)("bigint", treat_direct);
            result query_res1 = tr.prepared("find-by-id")(array_id).exec();

            if (query_res1.size() <= 0)
            {
                if (throwException) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
                } else {
                    return array_desc;
                }
            }
            ArrayID array_id = query_res1[0].at("id").as(int64_t());

            string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value, comment "
                " from \"array_attribute\" where array_id = $1 order by id";
            _connection->prepare(sql2, sql2)("integer", treat_direct);
            result query_res2 = tr.prepared(sql2)(array_id).exec();

            Attributes attributes;
            if (query_res2.size() > 0)
            {
                attributes.reserve(query_res2.size());
                for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
                {
                    Value defaultValue;
                    string defaultValueExpr = "";
                    int missingReason = i.at("default_missing_reason").as(int());
                    if (missingReason >= 0) {
                        defaultValue.setNull(missingReason);
                    } else {
                        defaultValueExpr = i.at("default_value").as(string());

                        // Evaluate expression if present and set default value
                        if (defaultValueExpr != "")
                        {
                            Expression expr = deserializePhysicalExpression(defaultValueExpr);
                            defaultValue = expr.evaluate();
                        }
                        // Else fallback to null or zero as before
                        else
                        {
                            defaultValue = Value(TypeLibrary::getType(i.at("type").as(TypeId())));
                            if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                            {
                                defaultValue.setNull();
                            }
                            else
                            {
                                defaultValue.setZero();
                            }
                        }
                    }
                    AttributeDesc att(
                        i.at("id").as(AttributeID()),
                        i.at("name").as(string()),
                        i.at("type").as (TypeId()),
                        i.at("flags").as(int16_t()),
                        i.at("default_compression_method").as(uint16_t()),
                        std::set<std::string>(),
                        i.at("reserve").as(int16_t()),
                        &defaultValue,
                        defaultValueExpr,
                        i.at("comment").as(string())
                        );

                    attributes.push_back(att);
                }
            }

            // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
            //      " from \"array_dimension\" where array_id = $1 order by id";

            string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap, type, source_array_name, comment "
                " from \"array_dimension\" where array_id = $1 order by id";
            _connection->prepare(sql3, sql3)("integer", treat_direct);
            result query_res3 = tr.prepared(sql3)(array_id).exec();

            Dimensions dimensions;
            if (query_res3.size() > 0)
            {
                attributes.reserve(query_res3.size());
                for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
                {
                    dimensions.push_back(
                        DimensionDesc(
                            i.at("name").as(string()),
                            i.at("startmin").as(int64_t()),
                            i.at("currstart").as(int64_t()),
                            i.at("currend").as(int64_t()),
                            i.at("endmax").as(int64_t()),
                            i.at("chunk_interval").as(size_t()),
                            i.at("chunk_overlap").as(size_t()),
                            i.at("type").as(string()),
                            i.at("source_array_name").as(string()),
                            i.at("comment").as(string())
                            ));
                }
            }
            array_desc = boost::shared_ptr<ArrayDesc>(new ArrayDesc(array_id,
                                                                    query_res1[0].at("name").as(string()),
                                                                    attributes,
                                                                    dimensions,
                                                                    query_res1[0].at("flags").as(int()),
                                                                    query_res1[0].at("comment").as(string())
                                                                    ));
            boost::shared_ptr<ArrayDesc>& elem = arrayDescByID[array_id];
            if (!elem) {
                elem = array_desc;
            } else {
                *elem = *array_desc;
            }
            array_desc = elem;

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting array";
        }
        return array_desc;
    }

    PartitioningSchema SystemCatalog::getPartitioningSchema(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getPartitioningSchema( id = " << array_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql = "select partitioning_schema from \"array\" where id = $1";
            _connection->prepare(sql, sql)("integer", treat_direct);

            result query_res = tr.prepared(sql)(array_id).exec();

            if (query_res.size() <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
            }

            return (PartitioningSchema)query_res[0].at("partitioning_schema").as(int());
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when adding array";
        }
    }

    bool SystemCatalog::deleteArray(const string &array_name)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        bool rc = false;
        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"array\" where name = $1 or name like $1||'@%' or name like $1||':%'";
            _connection->prepare("delete-array-name", sql1)("varchar", treat_string);
            result query_res = tr.prepared("delete-array-name")(array_name).exec();

            rc = (query_res.affected_rows() > 0);
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when deleting array";
        }
        return rc;
    }

    void SystemCatalog::deleteArray(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"array\" where id = $1";
            _connection->prepare("delete-array-id", sql1)("integer", treat_direct);
            tr.prepared("delete-array-id")(array_id).exec();

            string sql2 = "delete from \"array_version\" where array_id=$1";
            _connection->prepare("delete-array-version", sql2)("bigint", treat_direct);
            tr.prepared("delete-array-version")(array_id).exec();

            tr.commit();

            if (arrayDescByID.find(array_id) != arrayDescByID.end())
            {
                LOG4CXX_TRACE(logger, "Deleting array name = " << arrayDescByID[array_id]->getName());
                arrayDescByID.erase(array_id);
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when deleting array";
        }
    }

    void SystemCatalog::deleteArrayCache(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayCache( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        arrayDescByID.erase(array_id);
    }

    void SystemCatalog::deleteArrayCache(const string& array_name)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayCache( array_name = " << array_name << ")");
    }

    void SystemCatalog::cleanupCache()
    {
        ScopedMutexLock mutexLock(_pgLock);

        std::map<ArrayID, boost::shared_ptr<ArrayDesc> >::iterator it = arrayDescByID.begin();
        while (it != arrayDescByID.end())
        {
            if (!reloadArrayDesc(it->first, false))
            {
                arrayDescByID.erase(it++);
            }
            else
            {
                ++it;
            }
        }
    }

void SystemCatalog::deleteAllArrayVersionsFromCacheByName(const string& baseArrayName)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::deleteAllArrayVersionsFromCacheByName( baseArrayName = "
                  << baseArrayName << ")");

    ScopedMutexLock mutexLock(_pgLock);

    std::map<ArrayID, boost::shared_ptr<ArrayDesc> >::iterator it = arrayDescByID.begin();
    while (it != arrayDescByID.end())
    {
        VersionID ver(0);
        const string& arrayName = it->second->getName();
        const std::string& versionName = splitArrayNameVersion(arrayName, ver);

        if (versionName == baseArrayName)
        {
            LOG4CXX_TRACE(logger, "SystemCatalog::deleteAllArrayVersionsFromCacheByName: array descriptor for '"
                           << arrayName << "' being removed");
            arrayDescByID.erase(it++);
        }
        else
        {
            ++it;
        }
    }
}

    VersionID SystemCatalog::createNewVersion(const ArrayID array_id, const ArrayID version_array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::createNewVersion( array_id = " << array_id << ")");
        VersionID version_id = (VersionID)-1;
        {
            ScopedMutexLock mutexLock(_pgLock);

            assert(_connection);

            try
            {
                work tr(*_connection);

                string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
                _connection->prepare(sql, sql)
                    ("bigint", treat_direct);
                result query_res = tr.prepared(sql)(array_id).exec();
                version_id = query_res[0].at("vid").as(uint64_t());

                version_id += 1;

                string sql1 = "insert into \"array_version\"(array_id, version_array_id, version_id, time_stamp)"
                    " values ($1, $2, $3, $4)";
                int64_t timestamp = time(NULL);
                _connection->prepare(sql1, sql1)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct);
                tr.prepared(sql1)(array_id)(version_array_id)(version_id)(timestamp).exec();


                tr.commit();
            }
            catch (const sql_error &e)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
            }
            catch (const Exception &e)
            {
                throw;
            }
            catch (const std::exception &e)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
            }
            catch (...)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                    "Unknown exception when creating a new version of an updateable array";
            }
        }
        // updateArrayBoundaries(array_id, getLowBoundary(version_array_id), getHighBoundary(version_array_id));
        return version_id;
    }

    void SystemCatalog::deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteVersion( array_id = " << array_id << ", version_id = " << version_id << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            _connection->prepare("delete-version", "delete from \"array_version\" where array_id=$1 and version_id = $2")
                ("bigint", treat_direct)
                ("bigint", treat_direct);
            tr.prepared("delete-version")(array_id)(version_id).exec();
            _connection->unprepare("delete-version");
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when deleting a version of an updateable array";
        }
    }
/*
** TODO: We will need to rework this so that we only need to go back to the
**       persistent meta-data store when the local cache is invalidated
**       due to a new version being created with an UPDATE. This can be
**       piggy-backed on a "heartbeat" message, which contains the latest
**       lamport clock value. Meta-data additions to the persistent store
**       will increment the lamport clock value, which will be propogated
**       throughout the nodes. When the local node's clock value < the
**       'global', and the local is obliged to check it's local catalogs,
**       then it can reload meta-data from the persistent store.
*/
    VersionID SystemCatalog::getLastVersion(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getLastVersion( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
            _connection->prepare("select-last-version", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-last-version")(array_id).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when getting last version ID for updateble array";
        }
        return 0;
    }

    VersionID SystemCatalog::lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::lookupVersionByTimestamp( array_id = " << array_id << ", timestamp = " << timestamp << ")");

        ScopedMutexLock mutexLock(_pgLock);

        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1 and time_stamp <= $2";
            _connection->prepare("select-version-by-timestamp", sql)
                ("bigint", treat_direct)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-version-by-timestamp")(array_id)(timestamp).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when looking up an updateble array's version by timestamp";
        }
        return 0;
    }

    std::vector<VersionDesc> SystemCatalog::getArrayVersions(const ArrayID array_id) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayVersions( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select \"version_array_id\", \"version_id\", \"time_stamp\" from \"array_version\" where \"array_id\"=$1 order by \"version_id\";";
            _connection->prepare("select-all-versions", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-all-versions")(array_id).exec();
            std::vector<VersionDesc> versions(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                versions[j++] = VersionDesc(i.at("version_array_id").as(uint64_t()), i.at("version_id").as(uint64_t()), i.at("time_stamp").as(time_t()));
            }
            tr.commit();
            return versions;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when retrieving versions of an updateable array";
        }
        return std::vector<VersionDesc>();
    }

    Coordinates SystemCatalog::getHighBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getHighBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select currEnd from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-high-boundary", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-high-boundary")(array_id).exec();
            Coordinates highBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                highBoundary[j++] = i.at("currEnd").as(int64_t());
            }

            if (0 == j)
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;

            tr.commit();
            return highBoundary;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when getting current upper dimension boundaries for array";
        }
        return Coordinates();
    }

    Coordinates SystemCatalog::getLowBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLowBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {
            work tr(*_connection);

            string sql = "select currStart from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-low-boundary", sql)
                ("bigint", treat_direct);
            result query_res = tr.prepared("select-low-boundary")(array_id).exec();
            Coordinates lowBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                lowBoundary[j++] = i.at("currStart").as(int64_t());
            }

            if (0 == j)
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;

            tr.commit();
            return lowBoundary;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when getting current lower dimension boundaries for array";
        }
        return Coordinates();
    }

    void SystemCatalog::updateArrayBoundaries(const ArrayID array_id, Coordinates const& low, Coordinates const& high)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::updateArrayBoundaries( array_id = " << array_id << ", low = [" << low << "], high = [" << high << "])");

        ScopedMutexLock mutexLock(_pgLock);
        try
        {

            work tr(*_connection);

            string sql1 = "update \"array_dimension\" set currStart=$1 where array_id=$2 and id=$3 and currStart>$1";
            _connection->prepare("update-low-boundary", sql1)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct);
            string sql2 = "update \"array_dimension\" set currEnd=$1 where array_id=$2 and id=$3 and currEnd<$1";
            _connection->prepare("update-high-boundary", sql2)
                ("bigint", treat_direct)
                ("bigint", treat_direct)
                ("int", treat_direct);
            for (int i = 0, n = low.size(); i < n; i++) {
                tr.prepared("update-low-boundary")(low[i])(array_id)(i).exec();
                tr.prepared("update-high-boundary")(high[i])(array_id)(i).exec();
            }
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when updating array low/high dimension coordinates";
        }
    }

    uint32_t SystemCatalog::getNumberOfNodes() const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNumberOfNodes()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);
        uint32_t n_nodes;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select count(*) as cnt from \"node\"");
            n_nodes = query_res[0].at("cnt").as(uint32_t());
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when get number of nodes";
        }
        return n_nodes;
    }

    NodeID SystemCatalog::addNode(const NodeDesc &node) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addNode( " << node << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        int64_t node_id = 0;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('node_id_seq')");
            node_id = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"node\"(node_id, host, port, online_since) values ($1, $2, $3, 'infinity')";
            _connection->prepare(sql1, sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string)
                ("varchar", treat_string);
            tr.prepared(sql1)(node_id)(node.getHost())(node.getPort()).exec();

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when adding node";
        }
        return node_id;
    }

    void SystemCatalog::getNodes(Nodes &nodes) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNodes()");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "select node_id, host, port, date_part('epoch', online_since)::bigint as ts from \"node\" order by node_id";
            result query_res = tr.exec(sql);

            if (query_res.size() > 0)
            {
                nodes.reserve(query_res.size());
                for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
                {
                    nodes.push_back(
                        NodeDesc(
                            i.at("node_id").as(uint64_t()),
                            i.at("host").as(string()),
                            i.at("port").as(uint16_t()),
                            i.at("ts").as(uint64_t()) ));

                }
            }

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting nodes list";
        }

        LOG4CXX_TRACE(logger, "Retrieved " << nodes.size() << " nodes from catalogs");
    }

    void SystemCatalog::getNode(const NodeID node_id, NodeDesc &node) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNode( node_id = " << node_id << " NodeDesc& )");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "select node_id, host, port, date_part('epoch', online_since)::bigint as ts from \"node\" where node_id = $1";
            _connection->prepare(sql, sql)("bigint", treat_direct);
            result query_res = tr.prepared(sql)(node_id).exec();

            if (query_res.size() <= 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_NODE_DOESNT_EXIST) << node_id;

            node = NodeDesc(
                query_res[0].at("node_id").as(uint64_t()),
                query_res[0].at("host").as(string()),
                query_res[0].at("port").as(uint16_t()),
                query_res[0].at("ts").as(uint64_t()) );

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when getting node";
        }
        LOG4CXX_TRACE(logger, "node_id = " << node_id << " is node " << node);
    }

    void SystemCatalog::markNodeOnline(const NodeID node_id, const std::string host, const uint16_t port) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::markNodeOnline( node_id = " << node_id << ", host = " << host << ", port = " << port << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "update \"node\" set host = $1, port = $2, online_since = 'now' where node_id = $3";
            _connection->prepare(sql, sql)
                ("varchar", treat_string)
                ("varchar", treat_string)
                ("bigint", treat_direct);

            tr.prepared(sql)(host)(port)(node_id).exec();

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when marking node online";
        }
    }

    void SystemCatalog::markNodeOffline(const NodeID node_id) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::markNodeOffline( node_id = " << node_id << ")");
        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql = "update \"node\" set online_since = 'infinity' where node_id = $1";
            _connection->prepare(sql, sql)
                ("bigint", treat_direct);

            tr.prepared(sql)(node_id).exec();

            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << "Unknown exception when marking node offline";
        }
    }

    void SystemCatalog::setDefaultCompressionMethod(const ArrayID array_id,
                                                    const AttributeID attr_id, const int16_t compressionMethod)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::setDefaultCompressionMethod( array_id = " << array_id << ", attr_id = " << attr_id << ", compressionMethod = " << compressionMethod << ")");
        ScopedMutexLock mutexLock(_pgLock);

        const boost::shared_ptr<ArrayDesc>& arrayDesc = getArrayDesc(array_id);
        assert(arrayDesc);

        assert(_connection);

        try
        {
            if (attr_id > arrayDesc->getAttributes().size())
            {
                stringstream ss;
                ss << "Attribute with id " << attr_id << " not exist in array '"
                   << arrayDesc->getName() << "'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ATTRIBUTE_DOESNT_EXIST) << attr_id << arrayDesc->getName();
            }

            work tr(*_connection);

            string sql = "update \"array_attribute\" set default_compression_method = $1 "
                " where array_id = $2 and id = $3";
            _connection->prepare(sql, sql)
                ("int", treat_direct)
                ("bigint", treat_direct)
                ("bigint", treat_direct);

            tr.prepared(sql)(compressionMethod)(array_id)(attr_id).exec();

            tr.commit();

            if (arrayDescByID.find(array_id) != arrayDescByID.end())
            {
                Attributes const& attributes = arrayDesc->getAttributes();
                Attributes cachedAttributes;
                cachedAttributes.reserve(attributes.size());
                for (size_t i = 0, n = attributes.size(); i < n; i++)
                {
                    AttributeDesc const& attr = attributes[i];
                    cachedAttributes[i] = ((AttributeID)i == attr_id)
                        ? AttributeDesc(i, attr.getName(), attr.getType(), attr.getFlags(),
                                        compressionMethod, std::set<std::string>(), attr.getReserve(), &attr.getDefaultValue(),
                                        attr.getDefaultValueExpr(), attr.getComment())
                        : attr;
                }
                const boost::shared_ptr<ArrayDesc> cachedArrayDesc(new ArrayDesc(array_id, arrayDesc->getName(), cachedAttributes,
                                                                                       arrayDesc->getDimensions(), arrayDesc->getFlags(),
                                                                                       arrayDesc->getComment()));
                boost::shared_ptr<ArrayDesc>& elem = arrayDescByID[array_id];
                if (!elem) {
                    elem = cachedArrayDesc;
                } else {
                    *elem = *cachedArrayDesc;
                }
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when setting default compression method to attribute";
        }
    }


    void SystemCatalog::connect(const std::string& connectionString)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::connect( connect string ='" << connectionString << ")");

        if (!PQisthreadsafe())
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_LIBPQ_NOT_THREADSAFE);

        try
        {
            _connection = new pqxx::connection(connectionString);

            work tr(*_connection);
            result query_res = tr.exec("select count(*) from pg_tables where tablename = 'cluster'");
            _initialized = query_res[0].at("count").as(bool());

            if (_initialized)
            {
                result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
                _uuid = query_res[0].at("uuid").as(string());
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when connecting to system catalog";
        }
    }

    bool SystemCatalog::isConnected() const
    {
        if (_connection)
            return _connection->is_open();
        return false;
    }

    void SystemCatalog::addLibrary(const string& libraryName) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary( libraryName ='" << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('libraries_id_seq')");
            int64_t lid = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"libraries\"(id, name)"
                " values ($1, $2)";
            _connection->prepare("addLibrary", sql1)
                ("bigint", treat_direct)
                ("varchar", treat_string);
            tr.prepared("addLibrary")
                (lid)
                (libraryName).exec();
            _connection->unprepare("addLibrary");
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when adding library to catalog";
        }
    }

    void SystemCatalog::getLibraries(vector< string >& libraries) const
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLibraries ( &libraries )");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql1 = "select name from \"libraries\"";
            _connection->prepare("getLibraries", sql1);
            result query_res = tr.prepared("getLibraries").exec();
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i) {
                libraries.push_back(i.at("name").as(string()));
            }
            _connection->unprepare("getLibraries");
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when getting info about library from catalog";
        }

        LOG4CXX_TRACE(logger, "Loaded " << libraries.size() << " libraries.");
    }

    void SystemCatalog::removeLibrary(const string& libraryName) const
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::removeLibrary ( " << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"libraries\" where name = $1";
            _connection->prepare("removeLibrary", sql1)
                ("varchar", treat_string);
            tr.prepared("removeLibrary")
                (libraryName).exec();
            _connection->unprepare("removeLibrary");
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when removing library from catalog";
        }
    }


    SystemCatalog::SystemCatalog() :
    _initialized(false),
    _connection(NULL),
    _uuid("")
    {

    }

    SystemCatalog::~SystemCatalog()
    {
        if (_connection)
        {
            try
            {
                if (isConnected())
                {
                    _connection->disconnect();
                }
                delete _connection;
                _connection = NULL;
            }
            catch (...)
            {
                // FIXME: Log this error
                LOG4CXX_DEBUG(logger, "Error when disconnecting from PostgreSQL.");
            }
        }
    }

std::string SystemCatalog::getLockInsertSql(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   assert(lockDesc->getNodeRole() == LockDesc::COORD ||
          lockDesc->getNodeRole() == LockDesc::WORKER);
   string lockInsertSql;


   if (lockDesc->getLockMode() == LockDesc::RD) {
      if (lockDesc->getNodeRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " (array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where AVL.array_name=$1::VARCHAR and AVL.lock_mode>$9 and AVL.node_role=$10))";

      } else {
          assert(false);
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
      }
   } else if (lockDesc->getLockMode() == LockDesc::WR ||
              lockDesc->getLockMode() == LockDesc::CRT) {
      if (lockDesc->getNodeRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " (array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where AVL.array_name=$1::VARCHAR and AVL.lock_mode>$9))";

      } else if (lockDesc->getNodeRole() == LockDesc::WORKER) {
          if (lockDesc->getLockMode() == LockDesc::CRT) {
              assert(false);
              throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
          }          
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
         "  (select AVL.array_name, AVL.array_id, AVL.query_id, $3, AVL.array_version_id, AVL.array_version, $4, AVL.lock_mode"
         " from array_version_lock as AVL where AVL.array_name=$1::VARCHAR"
         " and AVL.query_id=$2 and AVL.node_role=1 and (AVL.lock_mode=$5 or AVL.lock_mode=$6))";

      }
   }  else if (lockDesc->getLockMode() == LockDesc::RM) {

      if (lockDesc->getNodeRole() == LockDesc::COORD) {
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
         "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
         "  (select array_name from array_version_lock where array_name=$1::VARCHAR))";
      } else {
         assert(false);
         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
      }
   }  else if (lockDesc->getLockMode() == LockDesc::RNF) {
        if (lockDesc->getNodeRole() == LockDesc::COORD) {
            lockInsertSql = "insert into array_version_lock"
            " ( array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
            "(select $1::VARCHAR,$2,$3,$4,$5,$6,$7,$8 where not exists"
            "  (select array_name from array_version_lock where array_name=$1::VARCHAR))";
      } else if (lockDesc->getNodeRole() == LockDesc::WORKER) {
         lockInsertSql = "insert into array_version_lock"
         " ( array_name, array_id, query_id, node_id, array_version_id, array_version, node_role, lock_mode)"
         "  (select AVL.array_name, AVL.array_id, AVL.query_id, $3, AVL.array_version_id, AVL.array_version, $4, AVL.lock_mode"
         " from array_version_lock as AVL where AVL.array_name=$1::VARCHAR"
         " and AVL.query_id=$2 and AVL.node_role=$5 and AVL.lock_mode=$6)";
      }
   } else {
      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "lock mode";
   }
   return lockInsertSql;
}

bool SystemCatalog::lockArray(const boost::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
   assert(lockDesc);
   LOG4CXX_TRACE(logger, "SystemCatalog::lockArray: "<<lockDesc->toString());
   try
   {
      assert(_connection);
      string lockInsertSql = getLockInsertSql(lockDesc);

      string lockTableSql = "LOCK TABLE array_version_lock";
      while (true)
      {
         {
            ScopedMutexLock mutexLock(_pgLock);
            work tr(*_connection);
            result query_res;

            _connection->prepare(lockTableSql, lockTableSql);
            tr.prepared(lockTableSql).exec();

            if (lockDesc->getLockMode() == LockDesc::RD) {

                if (lockDesc->getNodeRole() == LockDesc::COORD) {
                    string uniquePrefix("COORD-RD-");
                    _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                    ("varchar", treat_string)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct);

                    query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId())
                    (lockDesc->getNodeId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    ((int)lockDesc->getNodeRole())
                    ((int)lockDesc->getLockMode())
                    ((int)LockDesc::RD)
                    ((int)LockDesc::COORD).exec();
                } else { assert(false);}
            } else if (lockDesc->getLockMode() == LockDesc::WR
                       || lockDesc->getLockMode() == LockDesc::CRT) {

                if (lockDesc->getNodeRole() == LockDesc::COORD) {
                    string uniquePrefix("COORD-WR-");
                    _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                    ("varchar", treat_string)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct);

                    query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId())
                    (lockDesc->getNodeId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    ((int)lockDesc->getNodeRole())
                    ((int)lockDesc->getLockMode())
                    ((int)LockDesc::INVALID_MODE).exec();

                } else if (lockDesc->getNodeRole() == LockDesc::WORKER) {
                    assert(lockDesc->getLockMode() != LockDesc::CRT);
                    string uniquePrefix("WORKER-WR-");
                    _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                    ("varchar", treat_string)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct);

                    query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getArrayName())
                    (lockDesc->getQueryId())
                    (lockDesc->getNodeId())
                    ((int)LockDesc::WORKER)
                    ((int)LockDesc::WR)
                    ((int)LockDesc::CRT).exec();

                    if (query_res.affected_rows() == 1) {
                        string lockReadSql = "select array_id, array_version_id, array_version"
                        " from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and node_id=$3";

                        _connection->prepare(lockReadSql, lockReadSql)
                        ("varchar", treat_string)
                        ("bigint", treat_direct)
                        ("bigint", treat_direct);
                        
                        result query_res_read = tr.prepared(lockReadSql)
                        (lockDesc->getArrayName())
                        (lockDesc->getQueryId())
                        (lockDesc->getNodeId()).exec();

                        assert(query_res_read.size() == 1);

                        lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                        lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                        lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                    }
               } else { assert(false); }
            }  else if (lockDesc->getLockMode() == LockDesc::RM) {

               assert(lockDesc->getNodeRole() == LockDesc::COORD);

               string uniquePrefix("RM-");
               _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
               ("varchar", treat_string)
               ("bigint", treat_direct)
               ("bigint", treat_direct)
               ("bigint", treat_direct)
               ("bigint", treat_direct)
               ("bigint", treat_direct)
               ("integer", treat_direct)
               ("integer", treat_direct);

               query_res = tr.prepared(uniquePrefix+lockInsertSql)
               (lockDesc->getArrayName())
               (lockDesc->getArrayId())
               (lockDesc->getQueryId())
               (lockDesc->getNodeId())
               (lockDesc->getArrayVersionId())
               (lockDesc->getArrayVersion())
               ((int)lockDesc->getNodeRole())
               ((int)lockDesc->getLockMode()).exec();
            }  else if (lockDesc->getLockMode() == LockDesc::RNF) {
                if (lockDesc->getNodeRole() == LockDesc::COORD) {

                    string uniquePrefix("COORD-RNF-");
                    _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                    ("varchar", treat_string)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct);

                    query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId())
                    (lockDesc->getNodeId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    ((int)lockDesc->getNodeRole())
                    ((int)lockDesc->getLockMode()).exec();

                } else if (lockDesc->getNodeRole() == LockDesc::WORKER) {

                    string uniquePrefix("WORKER-RNF-");
                    _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                    ("varchar", treat_string)
                    ("bigint", treat_direct)
                    ("bigint", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct)
                    ("integer", treat_direct);

                    query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getArrayName())
                    (lockDesc->getQueryId())
                    (lockDesc->getNodeId())
                    ((int)LockDesc::WORKER)
                    ((int)LockDesc::COORD)
                    ((int)LockDesc::RNF).exec();
                } else { assert(false); }
            } else {
               assert(false);
            }
            if (query_res.affected_rows() == 1) {
               tr.commit();
               return true;
            }
            if (lockDesc->getNodeRole() == LockDesc::WORKER &&
                query_res.affected_rows() != 1) {
               // workers must error out immediately
               assert(query_res.affected_rows()==0);
               tr.commit();
               return false;
            }
            tr.commit();
         }
         if (errorChecker && !errorChecker()) {
             return false;
         }
         uint32_t nsec = _rng();
         nsec = 1 + nsec%WAIT_LOCK_TIMEOUT;
         sleep(nsec);
      }
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception locking array: "
      << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString());
   }
   return false;
}

bool SystemCatalog::unlockArray(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   LOG4CXX_TRACE(logger, "SystemCatalog::unlockArray: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockDeleteSql("delete from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and node_id=$3");

      {
         ScopedMutexLock mutexLock(_pgLock);

         work tr(*_connection);

         _connection->prepare(lockDeleteSql, lockDeleteSql)
         ("varchar", treat_string)
         ("bigint", treat_direct)
         ("bigint", treat_direct);

         result query_res = tr.prepared(lockDeleteSql)
         (lockDesc->getArrayName())
         (lockDesc->getQueryId())
         (lockDesc->getNodeId()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception unlocking array: "
      << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString());
   }
   return rc;
}

bool SystemCatalog::updateArrayLock(const boost::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   LOG4CXX_TRACE(logger, "SystemCatalog::updateArrayLock: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockUpdateSql = "update array_version_lock set array_id=$4, array_version_id=$5, array_version=$6, lock_mode=$7"
                             " where array_name=$1::VARCHAR and query_id=$2 and node_id=$3";
      {
         ScopedMutexLock mutexLock(_pgLock);

         work tr(*_connection);

         _connection->prepare(lockUpdateSql, lockUpdateSql)
         ("varchar", treat_string)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("bigint", treat_direct)
         ("integer", treat_direct);

         result query_res = tr.prepared(lockUpdateSql)
         (lockDesc->getArrayName())
         (lockDesc->getQueryId())
         (lockDesc->getNodeId())
         (lockDesc->getArrayId())
         (lockDesc->getArrayVersionId())
         (lockDesc->getArrayVersion())
         ((int)lockDesc->getLockMode()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception when updating array lock: "
      << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString());
   }
   return rc;
}

void SystemCatalog::readArrayLocks(const NodeID nodeId,
                                   std::list<boost::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<boost::shared_ptr<LockDesc> >& workerLocks)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks(nodeId = " << nodeId);

   ScopedMutexLock mutexLock(_pgLock);
   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql = "select array_name, array_id, query_id, array_version_id, array_version, node_role, lock_mode"
      " from array_version_lock where node_id=$1";
      _connection->prepare(sql, sql)("bigint", treat_direct);

      result query_res = tr.prepared(sql)(nodeId).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks: found "<< size <<" locks");

      for (size_t i=0; i < size; ++i) {
         boost::shared_ptr<LockDesc> lock( new LockDesc(query_res[i].at("array_name").as(string()),
                                                        query_res[i].at("query_id").as(QueryID()),
                                                        nodeId,
                                                        static_cast<LockDesc::NodeRole>(query_res[i].at("node_role").as(int())),
                                                        static_cast<LockDesc::LockMode>(query_res[i].at("lock_mode").as(int()))
                                                        ));
         lock->setArrayVersion(query_res[i].at("array_version").as(VersionID()));
         lock->setArrayId(query_res[i].at("array_id").as(ArrayID()));
         lock->setArrayVersionId(query_res[i].at("array_version_id").as(ArrayID()));

         if (lock->getNodeRole() == LockDesc::COORD) {
            coordLocks.push_back(lock);
         } else {
            workerLocks.push_back(lock);
         }
         LOG4CXX_TRACE(logger, lock->toString());
      }
      tr.commit();
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: node ID = " << nodeId);
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception when reading all node locks";
   }
}

uint32_t SystemCatalog::deleteArrayLocks(const NodeID& nodeId)
{
    return deleteArrayLocks(nodeId, INVALID_QUERY_ID);
}

uint32_t SystemCatalog::deleteArrayLocks(const NodeID& nodeId, const QueryID& queryId)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayLocks nodeId = "
                  << nodeId
                  << " queryId = "<<queryId);
   size_t numLocksDeleted = 0;
   ScopedMutexLock mutexLock(_pgLock);
   try
   {
      assert(_connection);

      string lockDeleteSql("delete from array_version_lock where node_id=$1");
      bool isQuerySpecified = (queryId != INVALID_QUERY_ID && queryId != 0);

      work tr(*_connection);

      result query_res;

      if (isQuerySpecified) {
          lockDeleteSql += " and query_id=$2";
          _connection->prepare(lockDeleteSql, lockDeleteSql)
          ("bigint", treat_direct)
          ("bigint", treat_direct);
          query_res = tr.prepared(lockDeleteSql)(nodeId)(queryId).exec();
      } else {
          _connection->prepare(lockDeleteSql, lockDeleteSql)
          ("bigint", treat_direct);
          query_res = tr.prepared(lockDeleteSql)(nodeId).exec();
      }
      numLocksDeleted = query_res.affected_rows();

      LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayLocks: deleted "
                    << numLocksDeleted
                    <<"locks for node " << nodeId);
      tr.commit();
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception when deleting all node locks";
   }
   return numLocksDeleted;
}

boost::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::checkForCoordinatorLock(const string& arrayName, const QueryID& queryId)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock:"
                 << " arrayName = " << arrayName
                 << " queryID = " << queryId);

   boost::shared_ptr<LockDesc> coordLock;

   ScopedMutexLock mutexLock(_pgLock);
   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql = "select array_id, node_id, array_version_id, array_version, lock_mode"
      " from array_version_lock where array_name=$1::VARCHAR and query_id=$2 and node_role=$3";
      _connection->prepare(sql, sql)
      ("varchar", treat_string)
      ("bigint", treat_direct)
      ("integer", treat_direct);

      result query_res = tr.prepared(sql)(arrayName)(queryId)((int)LockDesc::COORD).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock found "<< size <<" locks");

      assert(size < 2);
      if (size > 0) {
         coordLock = boost::shared_ptr<LockDesc>(new LockDesc(arrayName,
                                                              queryId,
                                                              query_res[0].at("node_id").as(NodeID()),
                                                              LockDesc::COORD,
                                                              static_cast<LockDesc::LockMode>(query_res[0].at("lock_mode").as(int()))
                                                              ));
         coordLock->setArrayVersion(query_res[0].at("array_version").as(VersionID()));
         coordLock->setArrayId(query_res[0].at("array_id").as(ArrayID()));
         coordLock->setArrayVersionId(query_res[0].at("array_version_id").as(ArrayID()));
         LOG4CXX_TRACE(logger, coordLock->toString());
      }
      tr.commit();
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception when checking for coordinator lock on"
      << arrayName << " for query "<< queryId ;
   }
   return coordLock;
}

void SystemCatalog::renameArray(const string &old_array_name, const string &new_array_name)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::renameArray( old name = "
                 << old_array_name << ")"
                 << "new name = " << new_array_name << ")");

   // replace all AAA, AAA:x, AAA@y with BBB, BBB:x, BBB@y correspondingly
   string renameDimSql = "update \"array_dimension\" set source_array_name=regexp_replace(source_array_name, '^'||$1::VARCHAR||'([:@].+)?$', $2::VARCHAR||E'\\\\1')";
   string renameSql = "update \"array\" set name=regexp_replace(name, '^'||$1::VARCHAR||'([:@].+)?$', $2::VARCHAR||E'\\\\1')";
   ScopedMutexLock mutexLock(_pgLock);
   assert(_connection);
   try
   {
      work tr(*_connection);

      _connection->prepare(renameDimSql, renameDimSql)("varchar", treat_string)("varchar", treat_string);
      tr.prepared(renameDimSql)(old_array_name)(new_array_name).exec();

      _connection->prepare(renameSql, renameSql)("varchar", treat_string)("varchar", treat_string);
      result query_res = tr.prepared(renameSql)(old_array_name)(new_array_name).exec();

      bool rc = (query_res.affected_rows() > 0);
      if (!rc) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << old_array_name;
      }
      tr.commit();
   }
   catch (const unique_violation& e)
   {
       LOG4CXX_ERROR(logger, "SystemCatalog::renameArray: unique constraint violation:"<< e.what());
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXIST) << new_array_name;
   }
   catch (const sql_error &e)
   {  
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const Exception &e)
   {
      throw;
   }
   catch (const std::exception &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   catch (...)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
      << "Unknown exception when renaming an array";
   }
}


} // namespace catalog

