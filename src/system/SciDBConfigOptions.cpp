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

#include "stdint.h"

#include "system/Config.h"
#include "SciDBConfigOptions.h"
#include <unistd.h>

using namespace std;

namespace scidb
{

void configHook(int32_t configOption)
{
    switch (configOption)
    {
        case CONFIG_CONFIGURATION_FILE:
            Config::getInstance()->setConfigFileName(
                Config::getInstance()->getOption<string>(CONFIG_CONFIGURATION_FILE));
            break;

        case CONFIG_HELP:
            cout << "Available options:" << endl
                << Config::getInstance()->getDescription() << endl;
            exit(0);
            break;

        case CONFIG_VERSION:
            cout << SCIDB_BUILD_INFO_STRING() << endl;
            exit(0);
            break;
    }
}

void initConfig(int argc, char* argv[])
{
    Config *cfg = Config::getInstance();

    cfg->addOption
        (CONFIG_PRECISION, 'w', "precision", "PRECISION", "", scidb::Config::INTEGER,
               "Precision for printing floating point numbers. Default is 6", 6, false)
        (CONFIG_CATALOG_CONNECTION_STRING, 'c', "catalog", "CATALOG", "", Config::STRING,
            "Catalog connection string. In order to create use utils/prepare-db.sh")
        (CONFIG_LOG4CXX_PROPERTIES, 'l', "log-properties", "LOG_PROPERTIES", "",
            Config::STRING, "Log4cxx properties file.", string(""), false)
        (CONFIG_COORDINATOR, 'k', "coordinator", "COORDINATOR", "", Config::BOOLEAN,
            "Option to start coordinator instance. It will works on default port or on port specified by port option.",
            false, false)
        (CONFIG_PORT, 'p', "port", "PORT", "", Config::INTEGER, "Set port for server. Default - any free port, but 1239 if coodinator.",
                0, false)
        (CONFIG_ADDRESS, 'i', "interface", "INTERFACE", "", Config::STRING, "Interface for listening connections.",
                string("0.0.0.0"), false)
        (CONFIG_REGISTER, 'r', "register", "", "", Config::BOOLEAN,
            "Register instance in system catalog.", false, false)
        (CONFIG_ASYNC_REPLICATION, 0, "async-replication", "", "", Config::BOOLEAN,
            "Asynchronous replication.", true, false)
        (CONFIG_RECOVER, 0, "recover", "", "", Config::INTEGER,
            "Recover instance.", -1, false)
        (CONFIG_REDUNDANCY, 0, "redundancy", "", "", Config::INTEGER,
            "Level of redundancy.", 0, false)
        (CONFIG_INITIALIZE, 0, "initialize", "", "", Config::BOOLEAN,
            "Initialize cluster.", false, false)
        (CONFIG_STORAGE_URL, 's', "storage", "STORAGE", "", Config::STRING, "Storage URL.",
                string("./storage.scidb"), false)
        (CONFIG_PLUGINS, 'u', "plugins", "PLUGINS", "", Config::STRING, "Plugins folder.",
            string(SCIDB_INSTALL_PREFIX()) + string("/lib/scidb/plugins"), false)
        (CONFIG_CACHE_SIZE, 'm', "cache", "CACHE", "", Config::INTEGER,
            "Size of storage cache (Mb).", 256, false)
        (CONFIG_CONFIGURATION_FILE, 'f', "config", "", "", Config::STRING,
                "Instance configuration file.", string(""), false)
        (CONFIG_HELP, 'h', "help", "", "", Config::BOOLEAN, "Show this text.",
                false, false)
        (CONFIG_SPARSE_CHUNK_INIT_SIZE, 0, "sparse-chunk-init-size", "SPARSE_CHUNK_INIT_SIZE", "", Config::REAL,
            "Default density for sparse arrays (0.01 corresponds to 1% density),"
            "SciDB uses this parameter to calculate size of memory which has to be preallocated in sparse chunk,",
            DEFAULT_SPARSE_CHUNK_INIT_SIZE, false)
        (CONFIG_DENSE_CHUNK_THRESHOLD, 0, "dense-chunk-threshold", "DENSE_CHUNK_THRESHOLD", "", Config::REAL,
            "Minimal ratio of filled elements of sparse chunk.", DEFAULT_DENSE_CHUNK_THRESHOLD, false)
        (CONFIG_SPARSE_CHUNK_THRESHOLD, 0, "sparse-chunk-threshold", "SPARSE_CHUNK_THRESHOLD", "", Config::REAL,
            "Maximal ratio of filled elements of sparse chunk.", 0.1, false)
        (CONFIG_STRING_SIZE_ESTIMATION, 0, "string-size-estimation", "STRING_SIZE_ESTIMATION", "", Config::INTEGER,
            "Average string size (bytes).", DEFAULT_STRING_SIZE_ESTIMATION, false)
        (CONFIG_CHUNK_CLUSTER_SIZE, 0, "chunk-segment-size", "CHUNK_SEGMENT_SIZE", "", Config::INTEGER,
         "Size of chunks segment (bytes).", 85*1024*1024, false)
        (CONFIG_READ_AHEAD_SIZE, 0, "read-ahead-size", "READ_AHEAD_SIZE", "", Config::INTEGER,
            "Total size of read ahead chunks (bytes).", 64*1024*1024, false)
        (CONFIG_DAEMONIZE, 'd', "daemon", "", "", Config::BOOLEAN, "Run scidb in background.",
                false, false)
        (CONFIG_SAVE_RAM, 0, "save-ram", "", "SAVE_RAM", Config::BOOLEAN, "Minimize memory footprint of SciDB.",
                false, false)
        (CONFIG_MEM_ARRAY_THRESHOLD, 'a', "mem-array-threshold", "MEM_ARRAY_THRESHOLD", "", Config::INTEGER,
                "Maximal size of memory used by temporary in-memory array (Mb)", (int)DEFAULT_MEM_THRESHOLD, false)
        (CONFIG_TMP_PATH, 0, "tmp-path", "", "TMP_PATH", Config::STRING, "Directory for SciDB temporary files",
                string("./tmp"), false)
        (CONFIG_EXEC_THREADS, 't', "threads", "EXEC_THREADS", "", Config::INTEGER,
                "Number of execution threads for concurrent processing of chunks of one query", 4, false)
        (CONFIG_PREFETCHED_CHUNKS, 'q', "prefetch-queue-size", "PREFETCHED_CHUNKS", "", Config::INTEGER,
                "Number of prefetch chunks for each query", 4, false)
        (CONFIG_MAX_JOBS, 'j', "jobs", "MAX_JOBS", "", Config::INTEGER,
                "Max. number of queries/jobs that can be processed in parallel", 5, false)
        (CONFIG_USED_CPU_LIMIT, 'x', "used-cpu-limit", "USED_CPU_LIMIT", "", Config::INTEGER,
                "Max. number of threads for concurrent processing of one chunk", 0, false)
        (CONFIG_MERGE_SORT_BUFFER, 0, "merge-sort-buffer", "MERGE_SORT_BUFFER", "", Config::INTEGER,
                "Maximal size for in-memory sort buffer (Mb)", 128, false)
        (CONFIG_NETWORK_BUFFER, 'n', "network-buffer", "NETWORK_BUFFER", "", Config::INTEGER,
                "Size of memory used for network buffers (Mb)", 512, false)
        (CONFIG_ASYNC_IO_BUFFER, 0, "async-io-buffer", "ASYNC_IO_BUFFER", "", Config::INTEGER,
                "Maximal size of connection output IO queue (Mb)", 64, false)
        (CONFIG_CHUNK_RESERVE, 0, "chunk-reserve", "CHUNK_RESERVE", "", Config::INTEGER, "Percent of chunks size preallocated for adding deltas", 0, false)
        (CONFIG_ENABLE_DELTA_ENCODING, 0, "enable-delta-encoding", "ENABLE_DELTA_ENCODING", "", Config::BOOLEAN, "True if system should attempt to compute delta chunk versions", false, false)
        (CONFIG_VERSION, 'V', "version", "", "", Config::BOOLEAN, "Version.",
                false, false)
        (CONFIG_STATISTICS_MONITOR, 0, "stat-monitor", "STAT_MONITOR", "", Config::INTEGER,
                "Statistics monitor type: 0 - none, 1 - Logger, 2 - Postgres", 0, false)
        (CONFIG_STATISTICS_MONITOR_PARAMS, 0, "stat-monitor-params", "STAT_MONITOR_PARAMS", "STAT_MONITOR_PARAMS",
            Config::STRING, "Parameters for statistics monitor: logger name or connection string", string(""), false)
        (CONFIG_LOG_LEVEL, 0, "log-level", "LOG_LEVEL", "LOG_LEVEL", Config::STRING,
         "Level for basic log4cxx logger. Ignored if log-properties option is used. Default level is ERROR", string("error"), false)
        (CONFIG_RECONNECT_TIMEOUT, 0, "reconnect-timeout", "RECONNECT_TIMEOUT", "", Config::INTEGER, "Time in seconds to wait before re-connecting to peer(s).",
       3, false)
        (CONFIG_LIVENESS_TIMEOUT, 0, "liveness-timeout", "LIVENESS_TIMEOUT", "", Config::INTEGER, "Time in seconds to wait before declaring a network-silent instance dead.",
       120, false)
        (CONFIG_NO_WATCHDOG, 0, "no-watchdog", "NO_WATCHDOG", "", Config::BOOLEAN, "Do not start a watch-dog process.",
                false, false)
        (CONFIG_PARALLEL_SORT, 0, "parallel-sort", "PARALLEL_SORT", "", Config::BOOLEAN, "Performs first phase of merge sort in parallel.",
                true, false)
        (CONFIG_RLE_CHUNK_FORMAT, 0, "rle-chunk-format", "RLE_CHUNK_FORMAT", "", Config::BOOLEAN, "Use RLE chunk format.",
                true, false)
        (CONFIG_TILE_SIZE, 0, "tile-size", "TILE_SIZE", "", Config::INTEGER, "Size of tile", 10000, false)
        (CONFIG_TILES_PER_CHUNK, 0, "tiles-per-chunk", "TILES_PER_CHUNK", "", Config::INTEGER, "Number of tiles per chunk", 100, false)
        (CONFIG_SYNC_IO_INTERVAL, 0, "sync-io-interval", "SYNC_IO_INTERVAL", "", Config::INTEGER, "Interval of time for io synchronization (milliseconds)", -1, false)
        (CONFIG_IO_LOG_THRESHOLD, 0, "io-log-threshold", "IO_LOG_THRESHOLD", "", Config::INTEGER, "Duration above which ios are logged (milliseconds)", -1, false)
        (CONFIG_OUTPUT_PROC_STATS, 0, "output-proc-stats", "OUTPUT_PROC_STATS", "", Config::BOOLEAN, "Output SciDB process statistics such as virtual memory usage to stderr",
                false, false)
        (CONFIG_MAX_MEMORY_LIMIT, 0, "max-memory-limit", "MAX_MEMORY_LIMIT", "", Config::INTEGER, "Maximum amount of memory the scidb process can take up (megabytes)", -1, false)

        (CONFIG_SMALL_MEMALLOC_SIZE, 0, "small-memalloc-size", "SMALL-MEMALLOC-SIZE", "", Config::INTEGER, "Maximum size of a memory allocation request which is considered small (in bytes). Larger memory allocation requests may be allocated according to a different policy.", 64 * 1024, false)

        (CONFIG_LARGE_MEMALLOC_LIMIT, 0, "large-memalloc-limit", "LARGE-MEMALLOC-LIMIT", "", Config::INTEGER, "Maximum number of large  (vs. small) memory allocations. The policy for doing large memory allocations may be different from the (default) policy used for small memory allocations. This parameter limits the number of outstanding allocations performed using the (non-default) large-size allocation policy.", std::numeric_limits<int>::max(), false)
    
        (CONFIG_STRICT_CACHE_LIMIT, 0, "strict-cache-limit", "STRICT_CACHE_LIMIT", "", Config::BOOLEAN, "Block thread if cache is overflown", false, false)
        (CONFIG_REPART_SEQ_SCAN_THRESHOLD, 0, "repart-seq-scan-threshold", "REPART_SEQ_SCAN_THRESHOLD", "", Config::INTEGER, "Number of chunks in array cause repart to use sequential scan through source array", 1000000, false)
        /*
          Use query "setopt('repart-algorithm', 'value') where value is
            * 'dense'
            * 'sparse'
            * 'auto'
          for select repart algorithm
         */
        (CONFIG_REPART_ALGORITHM, 0, "repart-algorithm",
            "REPART_ALGORITHM", "",
            getDefinition<RepartAlgorithm>(3),
         "Algorithm for repart", 0, false)
        (CONFIG_REPART_DENSE_OPEN_ONCE, 0, "repart-dense-open-once",
         "REPART_DENSE_OPEN_ONCE", "", Config::BOOLEAN,
         "Dense algorithm of repart will open every source chunk just once",
         false, false)
        (CONFIG_REPART_DISABLE_TILE_MODE, 0, "repart-disable-tile-mode",
         "REPART_DISABLE_TILE_MODE", "", Config::BOOLEAN,
         "Disable tile mode for repart operator",
         false, false)
        (CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE, 0, "replication-receive-queue-size", "REPLICATION_RECEIVE_QUEUE_SIZE", "", Config::INTEGER, "The length of incoming replication queue (across all connections)", 1000, false)
        (CONFIG_REPLICATION_SEND_QUEUE_SIZE, 0, "replication-send-queue-size", "REPLICATION_SEND_QUEUE_SIZE", "", Config::INTEGER, "The length of outgoing replication queue (across all connections)", 1000, false)
        (CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT, 0, "array-emptyable-by-default", "ARRAY_EMPTYABLE_BY_DEFAULT", "", Config::BOOLEAN, "Be default arrays are emptyable", true, false)
        (CONFIG_LOAD_SCAN_BUFFER, 0, "load-scan-buffer", "LOAD_SCAN_BUFFER", "", Config::INTEGER, "Number of MB for one input buffer used in InputScanner", 1, false)
        (CONFIG_MATERIALIZED_WINDOW_THRESHOLD, 0, "materialized-window-threshhold", "MATERIALIZED_WINDOW_THRESHHOLD", "", Config::INTEGER, "Size in Megabytes above which we will not materialize the input chunk to a window(...) operation", 128, false)
        (CONFIG_MPI_DIR, 0, "mpi-dir", "MPI_DIR", "", Config::STRING, "Location of MPI installation.", DEFAULT_MPI_DIR(), false)
        (CONFIG_MPI_IF, 0, "mpi-if", "MPI_IF", "", Config::STRING, "Network interface to use for MPI traffic", string(""), false)
        (CONFIG_MPI_TYPE, 0, "mpi-type", "MPI_TYPE", "", Config::STRING, "MPI installation type [mpich2-1.2 | mpich2-1.4].", DEFAULT_MPI_TYPE(), false)
        (CONFIG_CATALOG_RECONNECT_TRIES, 0, "catalog-reconnect-tries", "CONFIG_CATALOG_RECONNECT_TRIES", "", Config::INTEGER, "Count of tries of catalog reconnection", 5, false)
        ;

    cfg->addHook(configHook);

    cfg->parse(argc, argv, "");

    // By default redefine coordinator's port to 1239.
    if (!cfg->optionActivated(CONFIG_PORT) && cfg->getOption<bool>(CONFIG_COORDINATOR))
    {
        cfg->setOption(CONFIG_PORT, 1239);
    }

    // By default, change tmp-path to ./tmp
    if (!cfg->optionActivated(CONFIG_TMP_PATH)) {
        const string& storageConfigPath = cfg->getOption<string>(CONFIG_STORAGE_URL);
        string storageConfigDir = getDir(storageConfigPath);
        storageConfigDir += "/tmp";
        cfg->setOption(CONFIG_TMP_PATH, storageConfigDir);
    }
}

} // namespace
