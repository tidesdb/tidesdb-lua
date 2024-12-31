local db = require("libtidesdb_lua")

local code, message

local directory = "/tmp"
local name = "my_db"
local threshold = 1024*1024*64
local max_skip_list = 12
local prob_skip_list = 0.24
local enable_compression = true
local compression_algo = db.COMPRESS_SNAPPY
local enable_bloom_filter = true
local db_data_struct = db.TDB_MEMTABLE_SKIP_LIST

local key = "key"
local key_size = string.len(key)
local value = "value"
local value_size = string.len(value)
local ttl = 10

function test_open_close()
        code, message = db.open(directory)
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_create_and_drop_column()
        code, message = db.open(directory)
        assert(code == 0, message)
        code, message = db.create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db.drop_column_family(name)
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_put_and_get()
        code, message = db.open(directory)
        assert(code == 0, message)

        code, message = db.create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db.put(name, key, value, ttl)
        assert(code == 0, message)

        code, message, got_value = db.get(name, key)
        assert(code == 0, message)
        assert(got_value == value, "value mistmach!")

        code, message = db.drop_column_family(name)
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_put_and_delete()
        code, message = db.open(directory)
        assert(code == 0, message)

        code, message = db.create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db.put(name, key, value, ttl)
        assert(code == 0, message)

        code, message, got_value = db.delete(name, key)
        assert(code == 0, message)

        code, message = db.drop_column_family(name)
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_put_and_compact()
        code, message = db.open(directory)
        assert(code == 0, message)

        code, message = db.create_column_family(name,
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        for i=1,20 do
                code, message = db.put(name, i + '0', value, ttl)
                assert(code == 0, message)
        end

        code, message = db.compact_sstables(name, 2)

        code, message = db.drop_column_family(name)
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_list_column_families()
        code, message = db.open(directory)
        assert(code == 0, message)

        code, message = db.create_column_family("one",
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        code, message = db.create_column_family("two",
                                                threshold,
                                                max_skip_list,
                                                prob_skip_list,
                                                enable_compression,
                                                compression_algo,
                                                enable_bloom_filter,
                                                db_data_struct)
        assert(code == 0, message)

        list = db.list_column_families()
        assert(list == "one\ntwo\n")

        code, message = db.drop_column_family("one")
        assert(code == 0, message)

        code, message = db.drop_column_family("two")
        assert(code == 0, message)

        code, message = db.close()
        assert(code == 0, message)
end

function test_all()
        test_open_close()
        test_create_and_drop_column()
        test_put_and_get()
        test_put_and_delete()
        test_put_and_compact()
        test_list_column_families()
end

test_all()
