--[[
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Evgeny Kornev
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
]]
local lib = require("libtidesdb_lua")

local code, message

local directory = "tmp"
local name = "my_db"
local threshold = 1024*1024*64
local max_skip_list = 12
local prob_skip_list = 0.24
local enable_compression = true
local compression_algo = lib.COMPRESS_SNAPPY
local enable_bloom_filter = true
local db_data_struct = lib.TDB_MEMTABLE_SKIP_LIST

local key = "key"
local key_size = string.len(key)
local value = "value"
local value_size = string.len(value)
local ttl = 10

function test_open_close()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_create_and_drop_column()
        code, message, db = lib.open(directory)
        assert(code == 0, message)
        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db:drop_column_family(name)
        assert(code == 0, message)

        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_put_and_get()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db:put(name, key, value, ttl)
        assert(code == 0, message)

        code, message, got_value = db:get(name, key)
        assert(code == 0, message)
        assert(got_value == value, "value mistmach!")

        code, message = db:drop_column_family(name)
        assert(code == 0, message)

        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_put_and_delete()
        code, message = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db:put(name, key, value, ttl)
        assert(code == 0, message)

        code, message, got_value = db:delete(name, key)
        assert(code == 0, message)

        code, message = db:drop_column_family(name)
        assert(code == 0, message)

        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_put_and_compact()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        for i=1,20 do
                code, message = db:put(name, i + '0', value, ttl)
                assert(code == 0, message)
        end

        code, message = db:compact_sstables(name, 2)

        code, message = db:drop_column_family(name)
        assert(code == 0, message)

        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_list_column_families()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family("one",
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)
        assert(code == 0, message)

        code, message, list = db:list_column_families()
        assert(code == 0, message)
        assert(list == "one\n")

        code, message = db:drop_column_family("one")
        assert(code == 0, message)


        code, message = lib.close(db)
        assert(code == 0, message)
end

function test_txn_begin_and_free()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message, txn = lib.txn_begin(db, name)
        assert(code == 0, message)

        code, message = txn:free()
        assert(code == 0, message)

        db:drop_column_family(name)
        assert(code == 0, message)

        lib.close(db)
end

function test_txn_put_and_delete()
        code, message, db = lib.open(directory)
        assert(code == 0, message)

        code, message = db:create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message, txn = lib.txn_begin(db, name)
        assert(code == 0, message)

        code, message = txn:put(key, value, -1)
        assert(code == 0, message)

        code, message = txn:delete(key)
        assert(code == 0, message)

        code, message = txn:free()
        assert(code == 0, message)

        db:drop_column_family(name)
        assert(code == 0, message)

        lib.close(db)
end

function test_all()
        test_open_close()
        test_create_and_drop_column()
        test_put_and_get()
        test_put_and_delete()
        test_put_and_compact()
        test_list_column_families()
        test_txn_begin_and_free()
        test_txn_put_and_delete()
end

test_all()
