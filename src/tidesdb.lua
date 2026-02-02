--[[
TidesDB Lua Bindings v7+

Copyright (C) TidesDB
Original Author: Alex Gaetano Padula

Licensed under the Mozilla Public License, v. 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.mozilla.org/en-US/MPL/2.0/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
]]

local ffi = require("ffi")

ffi.cdef[[
    // Constants
    static const int TDB_MAX_COMPARATOR_NAME = 64;
    static const int TDB_MAX_COMPARATOR_CTX = 256;

    // Error codes
    static const int TDB_SUCCESS = 0;
    static const int TDB_ERR_MEMORY = -1;
    static const int TDB_ERR_INVALID_ARGS = -2;
    static const int TDB_ERR_NOT_FOUND = -3;
    static const int TDB_ERR_IO = -4;
    static const int TDB_ERR_CORRUPTION = -5;
    static const int TDB_ERR_EXISTS = -6;
    static const int TDB_ERR_CONFLICT = -7;
    static const int TDB_ERR_TOO_LARGE = -8;
    static const int TDB_ERR_MEMORY_LIMIT = -9;
    static const int TDB_ERR_INVALID_DB = -10;
    static const int TDB_ERR_UNKNOWN = -11;
    static const int TDB_ERR_LOCKED = -12;

    // Structures
    typedef struct {
        size_t write_buffer_size;
        size_t level_size_ratio;
        int min_levels;
        int dividing_level_offset;
        size_t klog_value_threshold;
        int compression_algorithm;
        int enable_bloom_filter;
        double bloom_fpr;
        int enable_block_indexes;
        int index_sample_ratio;
        int block_index_prefix_len;
        int sync_mode;
        uint64_t sync_interval_us;
        char comparator_name[64];
        char comparator_ctx_str[256];
        void* comparator_fn_cached;
        void* comparator_ctx_cached;
        int skip_list_max_level;
        float skip_list_probability;
        int default_isolation_level;
        uint64_t min_disk_space;
        int l1_file_count_trigger;
        int l0_queue_stall_threshold;
        int use_btree;
    } tidesdb_column_family_config_t;

    typedef struct {
        const char* db_path;
        int num_flush_threads;
        int num_compaction_threads;
        int log_level;
        size_t block_cache_size;
        size_t max_open_sstables;
        int log_to_file;
        size_t log_truncation_at;
    } tidesdb_config_t;

    typedef struct {
        int num_levels;
        size_t memtable_size;
        size_t* level_sizes;
        int* level_num_sstables;
        tidesdb_column_family_config_t* config;
        uint64_t total_keys;
        uint64_t total_data_size;
        double avg_key_size;
        double avg_value_size;
        uint64_t* level_key_counts;
        double read_amp;
        double hit_rate;
        int use_btree;
        uint64_t btree_total_nodes;
        uint32_t btree_max_height;
        double btree_avg_height;
    } tidesdb_stats_t;

    typedef struct {
        int enabled;
        size_t total_entries;
        size_t total_bytes;
        uint64_t hits;
        uint64_t misses;
        double hit_rate;
        size_t num_partitions;
    } tidesdb_cache_stats_t;

    // Database functions
    tidesdb_column_family_config_t tidesdb_default_column_family_config(void);
    tidesdb_config_t tidesdb_default_config(void);
    int tidesdb_open(tidesdb_config_t* config, void** db);
    int tidesdb_close(void* db);

    // Column family functions
    int tidesdb_create_column_family(void* db, const char* name, tidesdb_column_family_config_t* config);
    int tidesdb_drop_column_family(void* db, const char* name);
    int tidesdb_rename_column_family(void* db, const char* old_name, const char* new_name);
    void* tidesdb_get_column_family(void* db, const char* name);
    int tidesdb_list_column_families(void* db, char*** names, int* count);

    // Transaction functions
    int tidesdb_txn_begin(void* db, void** txn);
    int tidesdb_txn_begin_with_isolation(void* db, int isolation, void** txn);
    int tidesdb_txn_put(void* txn, void* cf, const uint8_t* key, size_t key_len, const uint8_t* value, size_t value_len, int ttl);
    int tidesdb_txn_get(void* txn, void* cf, const uint8_t* key, size_t key_len, uint8_t** value, size_t* value_len);
    int tidesdb_txn_delete(void* txn, void* cf, const uint8_t* key, size_t key_len);
    int tidesdb_txn_commit(void* txn);
    int tidesdb_txn_rollback(void* txn);
    void tidesdb_txn_free(void* txn);
    int tidesdb_txn_savepoint(void* txn, const char* name);
    int tidesdb_txn_rollback_to_savepoint(void* txn, const char* name);
    int tidesdb_txn_release_savepoint(void* txn, const char* name);

    // Iterator functions
    int tidesdb_iter_new(void* txn, void* cf, void** iter);
    int tidesdb_iter_seek_to_first(void* iter);
    int tidesdb_iter_seek_to_last(void* iter);
    int tidesdb_iter_seek(void* iter, const uint8_t* key, size_t key_len);
    int tidesdb_iter_seek_for_prev(void* iter, const uint8_t* key, size_t key_len);
    int tidesdb_iter_valid(void* iter);
    int tidesdb_iter_next(void* iter);
    int tidesdb_iter_prev(void* iter);
    int tidesdb_iter_key(void* iter, uint8_t** key, size_t* key_len);
    int tidesdb_iter_value(void* iter, uint8_t** value, size_t* value_len);
    void tidesdb_iter_free(void* iter);

    // Column family operations
    int tidesdb_compact(void* cf);
    int tidesdb_flush_memtable(void* cf);
    int tidesdb_is_flushing(void* cf);
    int tidesdb_is_compacting(void* cf);
    int tidesdb_get_stats(void* cf, tidesdb_stats_t** stats);
    void tidesdb_free_stats(tidesdb_stats_t* stats);
    int tidesdb_get_cache_stats(void* db, tidesdb_cache_stats_t* stats);

    // Backup operations
    int tidesdb_backup(void* db, const char* dir);

    // Configuration operations
    int tidesdb_cf_config_load_from_ini(const char* ini_file, const char* section_name, tidesdb_column_family_config_t* config);
    int tidesdb_cf_config_save_to_ini(const char* ini_file, const char* section_name, tidesdb_column_family_config_t* config);
    int tidesdb_cf_update_runtime_config(void* cf, tidesdb_column_family_config_t* new_config, int persist_to_disk);

    // Comparator operations
    typedef int (*tidesdb_comparator_fn)(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_register_comparator(void* db, const char* name, tidesdb_comparator_fn fn, const char* ctx_str, void* ctx);
    int tidesdb_get_comparator(void* db, const char* name, tidesdb_comparator_fn* fn, void** ctx);

    // Built-in comparator functions
    int tidesdb_comparator_memcmp(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_comparator_lexicographic(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_comparator_uint64(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_comparator_int64(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_comparator_reverse_memcmp(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);
    int tidesdb_comparator_case_insensitive(const uint8_t* key1, size_t key1_size, const uint8_t* key2, size_t key2_size, void* ctx);

    // Memory management
    void tidesdb_free(void* ptr);

    // C standard library
    void free(void* ptr);
]]

-- Load the TidesDB library
local lib
local function load_library()
    local lib_names
    if ffi.os == "Windows" then
        lib_names = {"tidesdb.dll", "libtidesdb.dll"}
    elseif ffi.os == "OSX" then
        lib_names = {"libtidesdb.dylib", "libtidesdb.so"}
    else
        lib_names = {"libtidesdb.so", "libtidesdb.so.1"}
    end

    local search_paths = {
        "",
        "/usr/local/lib/",
        "/usr/lib/",
        "/opt/homebrew/lib/",
        "/mingw64/lib/",
    }

    for _, path in ipairs(search_paths) do
        for _, lib_name in ipairs(lib_names) do
            local ok, result = pcall(ffi.load, path .. lib_name)
            if ok then
                return result
            end
        end
    end

    error(
        "Could not load TidesDB library. " ..
        "Please ensure libtidesdb is installed and in your library path. " ..
        "On Linux: /usr/local/lib or set LD_LIBRARY_PATH. " ..
        "On macOS: /usr/local/lib or /opt/homebrew/lib or set DYLD_LIBRARY_PATH. " ..
        "On Windows: ensure tidesdb.dll is in PATH or current directory."
    )
end

lib = load_library()

-- Module table
local tidesdb = {}

-- Constants
tidesdb.TDB_SUCCESS = 0
tidesdb.TDB_ERR_MEMORY = -1
tidesdb.TDB_ERR_INVALID_ARGS = -2
tidesdb.TDB_ERR_NOT_FOUND = -3
tidesdb.TDB_ERR_IO = -4
tidesdb.TDB_ERR_CORRUPTION = -5
tidesdb.TDB_ERR_EXISTS = -6
tidesdb.TDB_ERR_CONFLICT = -7
tidesdb.TDB_ERR_TOO_LARGE = -8
tidesdb.TDB_ERR_MEMORY_LIMIT = -9
tidesdb.TDB_ERR_INVALID_DB = -10
tidesdb.TDB_ERR_UNKNOWN = -11
tidesdb.TDB_ERR_LOCKED = -12

-- Compression algorithms
tidesdb.CompressionAlgorithm = {
    NO_COMPRESSION = 0,
    SNAPPY_COMPRESSION = 1,
    LZ4_COMPRESSION = 2,
    ZSTD_COMPRESSION = 3,
    LZ4_FAST_COMPRESSION = 4,
}

-- Sync modes
tidesdb.SyncMode = {
    SYNC_NONE = 0,
    SYNC_FULL = 1,
    SYNC_INTERVAL = 2,
}

-- Log levels
tidesdb.LogLevel = {
    LOG_DEBUG = 0,
    LOG_INFO = 1,
    LOG_WARN = 2,
    LOG_ERROR = 3,
    LOG_FATAL = 4,
    LOG_NONE = 99,
}

-- Isolation levels
tidesdb.IsolationLevel = {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED = 1,
    REPEATABLE_READ = 2,
    SNAPSHOT = 3,
    SERIALIZABLE = 4,
}

-- Error messages
local error_messages = {
    [tidesdb.TDB_ERR_MEMORY] = "memory allocation failed",
    [tidesdb.TDB_ERR_INVALID_ARGS] = "invalid arguments",
    [tidesdb.TDB_ERR_NOT_FOUND] = "not found",
    [tidesdb.TDB_ERR_IO] = "I/O error",
    [tidesdb.TDB_ERR_CORRUPTION] = "data corruption",
    [tidesdb.TDB_ERR_EXISTS] = "already exists",
    [tidesdb.TDB_ERR_CONFLICT] = "transaction conflict",
    [tidesdb.TDB_ERR_TOO_LARGE] = "key or value too large",
    [tidesdb.TDB_ERR_MEMORY_LIMIT] = "memory limit exceeded",
    [tidesdb.TDB_ERR_INVALID_DB] = "invalid database handle",
    [tidesdb.TDB_ERR_UNKNOWN] = "unknown error",
    [tidesdb.TDB_ERR_LOCKED] = "database is locked",
}

-- TidesDBError class
local TidesDBError = {}
TidesDBError.__index = TidesDBError

function TidesDBError.new(message, code)
    local self = setmetatable({}, TidesDBError)
    self.message = message
    self.code = code or tidesdb.TDB_ERR_UNKNOWN
    return self
end

function TidesDBError.from_code(code, context)
    local msg = error_messages[code] or "unknown error"
    if context and context ~= "" then
        msg = context .. ": " .. msg .. " (code: " .. code .. ")"
    else
        msg = msg .. " (code: " .. code .. ")"
    end
    return TidesDBError.new(msg, code)
end

function TidesDBError:__tostring()
    return "TidesDBError: " .. self.message
end

tidesdb.TidesDBError = TidesDBError

-- Helper function to check result and raise error
local function check_result(result, context)
    if result ~= tidesdb.TDB_SUCCESS then
        error(TidesDBError.from_code(result, context))
    end
end

-- Default configurations
function tidesdb.default_config()
    return {
        db_path = "",
        num_flush_threads = 2,
        num_compaction_threads = 2,
        log_level = tidesdb.LogLevel.LOG_INFO,
        block_cache_size = 64 * 1024 * 1024,
        max_open_sstables = 256,
        log_to_file = false,
        log_truncation_at = 24 * 1024 * 1024,
    }
end

function tidesdb.default_column_family_config()
    local c_config = lib.tidesdb_default_column_family_config()
    return {
        write_buffer_size = tonumber(c_config.write_buffer_size),
        level_size_ratio = tonumber(c_config.level_size_ratio),
        min_levels = c_config.min_levels,
        dividing_level_offset = c_config.dividing_level_offset,
        klog_value_threshold = tonumber(c_config.klog_value_threshold),
        compression_algorithm = c_config.compression_algorithm,
        enable_bloom_filter = c_config.enable_bloom_filter ~= 0,
        bloom_fpr = c_config.bloom_fpr,
        enable_block_indexes = c_config.enable_block_indexes ~= 0,
        index_sample_ratio = c_config.index_sample_ratio,
        block_index_prefix_len = c_config.block_index_prefix_len,
        sync_mode = c_config.sync_mode,
        sync_interval_us = tonumber(c_config.sync_interval_us),
        comparator_name = ffi.string(c_config.comparator_name),
        skip_list_max_level = c_config.skip_list_max_level,
        skip_list_probability = c_config.skip_list_probability,
        default_isolation_level = c_config.default_isolation_level,
        min_disk_space = tonumber(c_config.min_disk_space),
        l1_file_count_trigger = c_config.l1_file_count_trigger,
        l0_queue_stall_threshold = c_config.l0_queue_stall_threshold,
        use_btree = c_config.use_btree ~= 0,
    }
end

-- Convert Lua config to C struct
local function config_to_c_struct(config)
    local c_config = ffi.new("tidesdb_column_family_config_t")
    c_config.write_buffer_size = config.write_buffer_size or 64 * 1024 * 1024
    c_config.level_size_ratio = config.level_size_ratio or 10
    c_config.min_levels = config.min_levels or 5
    c_config.dividing_level_offset = config.dividing_level_offset or 2
    c_config.klog_value_threshold = config.klog_value_threshold or 512
    c_config.compression_algorithm = config.compression_algorithm or tidesdb.CompressionAlgorithm.LZ4_COMPRESSION
    c_config.enable_bloom_filter = config.enable_bloom_filter and 1 or 0
    c_config.bloom_fpr = config.bloom_fpr or 0.01
    c_config.enable_block_indexes = config.enable_block_indexes and 1 or 0
    c_config.index_sample_ratio = config.index_sample_ratio or 1
    c_config.block_index_prefix_len = config.block_index_prefix_len or 16
    c_config.sync_mode = config.sync_mode or tidesdb.SyncMode.SYNC_INTERVAL
    c_config.sync_interval_us = config.sync_interval_us or 128000
    c_config.skip_list_max_level = config.skip_list_max_level or 12
    c_config.skip_list_probability = config.skip_list_probability or 0.25
    c_config.default_isolation_level = config.default_isolation_level or tidesdb.IsolationLevel.READ_COMMITTED
    c_config.min_disk_space = config.min_disk_space or 100 * 1024 * 1024
    c_config.l1_file_count_trigger = config.l1_file_count_trigger or 4
    c_config.l0_queue_stall_threshold = config.l0_queue_stall_threshold or 20
    c_config.use_btree = config.use_btree and 1 or 0

    local name = config.comparator_name or "memcmp"
    local name_len = math.min(#name, 63)
    ffi.copy(c_config.comparator_name, name, name_len)
    c_config.comparator_name[name_len] = 0

    return c_config
end

-- Iterator class
local Iterator = {}
Iterator.__index = Iterator

function Iterator.new(iter_ptr)
    local self = setmetatable({}, Iterator)
    self._iter = iter_ptr
    self._closed = false
    return self
end

function Iterator:seek_to_first()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local result = lib.tidesdb_iter_seek_to_first(self._iter)
    check_result(result, "failed to seek to first")
end

function Iterator:seek_to_last()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local result = lib.tidesdb_iter_seek_to_last(self._iter)
    check_result(result, "failed to seek to last")
end

function Iterator:seek(key)
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local key_len = #key
    local result = lib.tidesdb_iter_seek(self._iter, key, key_len)
    check_result(result, "failed to seek")
end

function Iterator:seek_for_prev(key)
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local key_len = #key
    local result = lib.tidesdb_iter_seek_for_prev(self._iter, key, key_len)
    check_result(result, "failed to seek for prev")
end

function Iterator:valid()
    if self._closed then
        return false
    end
    return lib.tidesdb_iter_valid(self._iter) ~= 0
end

function Iterator:next()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    lib.tidesdb_iter_next(self._iter)
end

function Iterator:prev()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    lib.tidesdb_iter_prev(self._iter)
end

function Iterator:key()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local key_ptr = ffi.new("uint8_t*[1]")
    local key_size = ffi.new("size_t[1]")
    local result = lib.tidesdb_iter_key(self._iter, key_ptr, key_size)
    check_result(result, "failed to get key")
    return ffi.string(key_ptr[0], key_size[0])
end

function Iterator:value()
    if self._closed then
        error(TidesDBError.new("Iterator is closed"))
    end
    local value_ptr = ffi.new("uint8_t*[1]")
    local value_size = ffi.new("size_t[1]")
    local result = lib.tidesdb_iter_value(self._iter, value_ptr, value_size)
    check_result(result, "failed to get value")
    return ffi.string(value_ptr[0], value_size[0])
end

function Iterator:close()
    if not self._closed and self._iter ~= nil then
        lib.tidesdb_iter_free(self._iter)
        self._closed = true
    end
end

function Iterator:free()
    self:close()
end

tidesdb.Iterator = Iterator

-- ColumnFamily class
local ColumnFamily = {}
ColumnFamily.__index = ColumnFamily

function ColumnFamily.new(cf_ptr, name)
    local self = setmetatable({}, ColumnFamily)
    self._cf = cf_ptr
    self.name = name
    return self
end

function ColumnFamily:compact()
    local result = lib.tidesdb_compact(self._cf)
    check_result(result, "failed to compact column family")
end

function ColumnFamily:flush_memtable()
    local result = lib.tidesdb_flush_memtable(self._cf)
    check_result(result, "failed to flush memtable")
end

function ColumnFamily:is_flushing()
    return lib.tidesdb_is_flushing(self._cf) ~= 0
end

function ColumnFamily:is_compacting()
    return lib.tidesdb_is_compacting(self._cf) ~= 0
end

function ColumnFamily:update_runtime_config(config, persist_to_disk)
    if persist_to_disk == nil then
        persist_to_disk = true
    end
    local c_config = config_to_c_struct(config)
    local result = lib.tidesdb_cf_update_runtime_config(self._cf, c_config, persist_to_disk and 1 or 0)
    check_result(result, "failed to update runtime config")
end

function ColumnFamily:get_stats()
    local stats_ptr = ffi.new("tidesdb_stats_t*[1]")
    local result = lib.tidesdb_get_stats(self._cf, stats_ptr)
    check_result(result, "failed to get stats")

    local c_stats = stats_ptr[0]
    local level_sizes = {}
    local level_num_sstables = {}

    if c_stats.num_levels > 0 then
        if c_stats.level_sizes ~= nil then
            for i = 0, c_stats.num_levels - 1 do
                table.insert(level_sizes, tonumber(c_stats.level_sizes[i]))
            end
        end
        if c_stats.level_num_sstables ~= nil then
            for i = 0, c_stats.num_levels - 1 do
                table.insert(level_num_sstables, c_stats.level_num_sstables[i])
            end
        end
    end

    local config = nil
    if c_stats.config ~= nil then
        local c_cfg = c_stats.config
        config = {
            write_buffer_size = tonumber(c_cfg.write_buffer_size),
            level_size_ratio = tonumber(c_cfg.level_size_ratio),
            min_levels = c_cfg.min_levels,
            dividing_level_offset = c_cfg.dividing_level_offset,
            klog_value_threshold = tonumber(c_cfg.klog_value_threshold),
            compression_algorithm = c_cfg.compression_algorithm,
            enable_bloom_filter = c_cfg.enable_bloom_filter ~= 0,
            bloom_fpr = c_cfg.bloom_fpr,
            enable_block_indexes = c_cfg.enable_block_indexes ~= 0,
            index_sample_ratio = c_cfg.index_sample_ratio,
            block_index_prefix_len = c_cfg.block_index_prefix_len,
            sync_mode = c_cfg.sync_mode,
            sync_interval_us = tonumber(c_cfg.sync_interval_us),
            comparator_name = ffi.string(c_cfg.comparator_name),
            skip_list_max_level = c_cfg.skip_list_max_level,
            skip_list_probability = c_cfg.skip_list_probability,
            default_isolation_level = c_cfg.default_isolation_level,
            min_disk_space = tonumber(c_cfg.min_disk_space),
            l1_file_count_trigger = c_cfg.l1_file_count_trigger,
            l0_queue_stall_threshold = c_cfg.l0_queue_stall_threshold,
            use_btree = c_cfg.use_btree ~= 0,
        }
    end

    local level_key_counts = {}
    if c_stats.num_levels > 0 and c_stats.level_key_counts ~= nil then
        for i = 0, c_stats.num_levels - 1 do
            table.insert(level_key_counts, tonumber(c_stats.level_key_counts[i]))
        end
    end

    local stats = {
        num_levels = c_stats.num_levels,
        memtable_size = tonumber(c_stats.memtable_size),
        level_sizes = level_sizes,
        level_num_sstables = level_num_sstables,
        config = config,
        total_keys = tonumber(c_stats.total_keys),
        total_data_size = tonumber(c_stats.total_data_size),
        avg_key_size = c_stats.avg_key_size,
        avg_value_size = c_stats.avg_value_size,
        level_key_counts = level_key_counts,
        read_amp = c_stats.read_amp,
        hit_rate = c_stats.hit_rate,
        use_btree = c_stats.use_btree ~= 0,
        btree_total_nodes = tonumber(c_stats.btree_total_nodes),
        btree_max_height = c_stats.btree_max_height,
        btree_avg_height = c_stats.btree_avg_height,
    }

    lib.tidesdb_free_stats(stats_ptr[0])
    return stats
end

tidesdb.ColumnFamily = ColumnFamily

-- Transaction class
local Transaction = {}
Transaction.__index = Transaction

function Transaction.new(txn_ptr)
    local self = setmetatable({}, Transaction)
    self._txn = txn_ptr
    self._closed = false
    self._committed = false
    return self
end

function Transaction:put(cf, key, value, ttl)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    ttl = ttl or -1
    local key_len = #key
    local value_len = #value

    local result = lib.tidesdb_txn_put(self._txn, cf._cf, key, key_len, value, value_len, ttl)
    check_result(result, "failed to put key-value pair")
end

function Transaction:get(cf, key)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end

    local key_len = #key
    local value_ptr = ffi.new("uint8_t*[1]")
    local value_size = ffi.new("size_t[1]")

    local result = lib.tidesdb_txn_get(self._txn, cf._cf, key, key_len, value_ptr, value_size)
    check_result(result, "failed to get value")

    local value = ffi.string(value_ptr[0], value_size[0])
    ffi.C.free(value_ptr[0])
    return value
end

function Transaction:delete(cf, key)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local key_len = #key
    local result = lib.tidesdb_txn_delete(self._txn, cf._cf, key, key_len)
    check_result(result, "failed to delete key")
end

function Transaction:commit()
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local result = lib.tidesdb_txn_commit(self._txn)
    check_result(result, "failed to commit transaction")
    self._committed = true
end

function Transaction:rollback()
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local result = lib.tidesdb_txn_rollback(self._txn)
    check_result(result, "failed to rollback transaction")
end

function Transaction:savepoint(name)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local result = lib.tidesdb_txn_savepoint(self._txn, name)
    check_result(result, "failed to create savepoint")
end

function Transaction:rollback_to_savepoint(name)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local result = lib.tidesdb_txn_rollback_to_savepoint(self._txn, name)
    check_result(result, "failed to rollback to savepoint")
end

function Transaction:release_savepoint(name)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end
    if self._committed then
        error(TidesDBError.new("Transaction already committed"))
    end

    local result = lib.tidesdb_txn_release_savepoint(self._txn, name)
    check_result(result, "failed to release savepoint")
end

function Transaction:new_iterator(cf)
    if self._closed then
        error(TidesDBError.new("Transaction is closed"))
    end

    local iter_ptr = ffi.new("void*[1]")
    local result = lib.tidesdb_iter_new(self._txn, cf._cf, iter_ptr)
    check_result(result, "failed to create iterator")

    return Iterator.new(iter_ptr[0])
end

function Transaction:close()
    if not self._closed and self._txn ~= nil then
        lib.tidesdb_txn_free(self._txn)
        self._closed = true
    end
end

function Transaction:free()
    self:close()
end

tidesdb.Transaction = Transaction

-- TidesDB class
local TidesDB = {}
TidesDB.__index = TidesDB

function TidesDB.new(config)
    local self = setmetatable({}, TidesDB)
    self._db = nil
    self._closed = false

    -- Ensure db_path exists
    local path = config.db_path
    os.execute("mkdir -p " .. path)

    -- Store path to prevent GC
    self._path = path

    local c_config = ffi.new("tidesdb_config_t")
    c_config.db_path = path
    c_config.num_flush_threads = config.num_flush_threads or 2
    c_config.num_compaction_threads = config.num_compaction_threads or 2
    c_config.log_level = config.log_level or tidesdb.LogLevel.LOG_INFO
    c_config.block_cache_size = config.block_cache_size or 64 * 1024 * 1024
    c_config.max_open_sstables = config.max_open_sstables or 256
    c_config.log_to_file = config.log_to_file and 1 or 0
    c_config.log_truncation_at = config.log_truncation_at or 24 * 1024 * 1024

    local db_ptr = ffi.new("void*[1]")
    local result = lib.tidesdb_open(c_config, db_ptr)
    check_result(result, "failed to open database")

    self._db = db_ptr[0]
    return self
end

function TidesDB.open(path, options)
    options = options or {}
    local config = {
        db_path = path,
        num_flush_threads = options.num_flush_threads or 2,
        num_compaction_threads = options.num_compaction_threads or 2,
        log_level = options.log_level or tidesdb.LogLevel.LOG_INFO,
        block_cache_size = options.block_cache_size or 64 * 1024 * 1024,
        max_open_sstables = options.max_open_sstables or 256,
        log_to_file = options.log_to_file or false,
        log_truncation_at = options.log_truncation_at or 24 * 1024 * 1024,
    }
    return TidesDB.new(config)
end

function TidesDB:close()
    if not self._closed and self._db ~= nil then
        local db_ptr = self._db
        self._db = nil
        self._closed = true
        local result = lib.tidesdb_close(db_ptr)
        check_result(result, "failed to close database")
    end
end

function TidesDB:create_column_family(name, config)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    if config == nil then
        config = tidesdb.default_column_family_config()
    end

    local c_config = config_to_c_struct(config)
    local result = lib.tidesdb_create_column_family(self._db, name, c_config)
    check_result(result, "failed to create column family")
end

function TidesDB:drop_column_family(name)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local result = lib.tidesdb_drop_column_family(self._db, name)
    check_result(result, "failed to drop column family")
end

function TidesDB:rename_column_family(old_name, new_name)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local result = lib.tidesdb_rename_column_family(self._db, old_name, new_name)
    check_result(result, "failed to rename column family")
end

function TidesDB:get_column_family(name)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local cf_ptr = lib.tidesdb_get_column_family(self._db, name)
    if cf_ptr == nil then
        error(TidesDBError.new("Column family not found: " .. name, tidesdb.TDB_ERR_NOT_FOUND))
    end

    return ColumnFamily.new(cf_ptr, name)
end

function TidesDB:list_column_families()
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local names_ptr = ffi.new("char**[1]")
    local count = ffi.new("int[1]")

    local result = lib.tidesdb_list_column_families(self._db, names_ptr, count)
    check_result(result, "failed to list column families")

    if count[0] == 0 then
        return {}
    end

    local names = {}
    for i = 0, count[0] - 1 do
        local str_ptr = names_ptr[0][i]
        if str_ptr ~= nil then
            table.insert(names, ffi.string(str_ptr))
            ffi.C.free(str_ptr)
        end
    end
    ffi.C.free(names_ptr[0])

    return names
end

function TidesDB:begin_txn()
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local txn_ptr = ffi.new("void*[1]")
    local result = lib.tidesdb_txn_begin(self._db, txn_ptr)
    check_result(result, "failed to begin transaction")

    return Transaction.new(txn_ptr[0])
end

function TidesDB:begin_txn_with_isolation(isolation)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local txn_ptr = ffi.new("void*[1]")
    local result = lib.tidesdb_txn_begin_with_isolation(self._db, isolation, txn_ptr)
    check_result(result, "failed to begin transaction with isolation")

    return Transaction.new(txn_ptr[0])
end

function TidesDB:get_cache_stats()
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local c_stats = ffi.new("tidesdb_cache_stats_t")
    local result = lib.tidesdb_get_cache_stats(self._db, c_stats)
    check_result(result, "failed to get cache stats")

    return {
        enabled = c_stats.enabled ~= 0,
        total_entries = tonumber(c_stats.total_entries),
        total_bytes = tonumber(c_stats.total_bytes),
        hits = tonumber(c_stats.hits),
        misses = tonumber(c_stats.misses),
        hit_rate = c_stats.hit_rate,
        num_partitions = tonumber(c_stats.num_partitions),
    }
end

function TidesDB:backup(dir)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local result = lib.tidesdb_backup(self._db, dir)
    check_result(result, "failed to create backup")
end

function TidesDB:register_comparator(name, fn, ctx_str, ctx)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local result = lib.tidesdb_register_comparator(self._db, name, fn, ctx_str, ctx)
    check_result(result, "failed to register comparator")
end

function TidesDB:get_comparator(name)
    if self._closed then
        error(TidesDBError.new("Database is closed"))
    end

    local fn_ptr = ffi.new("tidesdb_comparator_fn[1]")
    local ctx_ptr = ffi.new("void*[1]")
    local result = lib.tidesdb_get_comparator(self._db, name, fn_ptr, ctx_ptr)
    check_result(result, "failed to get comparator")

    return fn_ptr[0], ctx_ptr[0]
end

tidesdb.TidesDB = TidesDB

-- Configuration file operations
function tidesdb.load_config_from_ini(ini_file, section_name)
    local c_config = ffi.new("tidesdb_column_family_config_t")
    local result = lib.tidesdb_cf_config_load_from_ini(ini_file, section_name, c_config)
    check_result(result, "failed to load config from INI")

    return {
        write_buffer_size = tonumber(c_config.write_buffer_size),
        level_size_ratio = tonumber(c_config.level_size_ratio),
        min_levels = c_config.min_levels,
        dividing_level_offset = c_config.dividing_level_offset,
        klog_value_threshold = tonumber(c_config.klog_value_threshold),
        compression_algorithm = c_config.compression_algorithm,
        enable_bloom_filter = c_config.enable_bloom_filter ~= 0,
        bloom_fpr = c_config.bloom_fpr,
        enable_block_indexes = c_config.enable_block_indexes ~= 0,
        index_sample_ratio = c_config.index_sample_ratio,
        block_index_prefix_len = c_config.block_index_prefix_len,
        sync_mode = c_config.sync_mode,
        sync_interval_us = tonumber(c_config.sync_interval_us),
        comparator_name = ffi.string(c_config.comparator_name),
        skip_list_max_level = c_config.skip_list_max_level,
        skip_list_probability = c_config.skip_list_probability,
        default_isolation_level = c_config.default_isolation_level,
        min_disk_space = tonumber(c_config.min_disk_space),
        l1_file_count_trigger = c_config.l1_file_count_trigger,
        l0_queue_stall_threshold = c_config.l0_queue_stall_threshold,
        use_btree = c_config.use_btree ~= 0,
    }
end

function tidesdb.save_config_to_ini(ini_file, section_name, config)
    local c_config = config_to_c_struct(config)
    local result = lib.tidesdb_cf_config_save_to_ini(ini_file, section_name, c_config)
    check_result(result, "failed to save config to INI")
end

-- Version
tidesdb._VERSION = "0.3.0"

return tidesdb
