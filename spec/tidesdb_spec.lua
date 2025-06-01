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
package.cpath = 'tidesdb-lua/build/?.so;build/?.so;' .. package.cpath

describe("load", function()
  it("should open tidesdb_lua", function()
    assert.truthy(require("libtidesdb_lua"))
  end)
end)

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

local key = "key"
local key_size = string.len(key)
local value = "value"
local value_size = string.len(value)
local ttl = 10

-- Setup test directory
os.execute("rm -rf " .. directory)
os.execute("mkdir -p " .. directory)
os.execute("chmod 777 " .. directory)

describe("load", function()
  it("should open tidesdb_lua", function()
    assert.truthy(require("libtidesdb_lua"))
  end)
end)

describe("open and close", function()
  it("should open and close db", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("create and drop column", function()
  it("should create and drop column", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("put and get", function()
  it("should put and get", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message = db:put(name, key, value, ttl)
    assert.is_equal(0, code)

    code, message, got_value = db:get(name, key)
    assert.is_equal(0, code)
    assert.is_equal(got_value, value)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("put and delete", function()
  it("should put and delete", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message = db:put(name, key, value, ttl)
    assert.is_equal(0, code)

    code, message, got_value = db:delete(name, key)
    assert.is_equal(0, code)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("list column families", function()
  it("should list column families", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message, list = db:list_column_families()
    assert.are_same(list, "my_db\n")

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("transactions begin and free", function()
  it("should begin and free transactions", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message, txn = lib.txn_begin(db, name)
    assert.is_equal(0, code)

    code, message = txn:free()
    assert.is_equal(0, code)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("transactions put and delete", function()
  it("should put and delete transactions", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    code, message, txn = lib.txn_begin(db, name)
    assert.is_equal(0, code)

    code, message = txn:put(key, value, -1)
    assert.is_equal(0, code)

    code, message = txn:delete(key)
    assert.is_equal(0, code)

    code, message = txn:free()
    assert.is_equal(0, code)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)

describe("cursor operations", function()
  it("should perform cursor operations", function()
    assert.truthy(lib.open(directory))

    local code, message, db = lib.open(directory)
    assert.is_equal(0, code)

    code, message = db:create_column_family(name,
                                            threshold,
                                            max_skip_list,
                                            prob_skip_list,
                                            enable_compression,
                                            compression_algo,
                                            enable_bloom_filter)
    assert.is_equal(0, code)

    -- Insert test data
    for i=1,5 do
      code, message = db:put(name, "key" .. i, "value" .. i, ttl)
      assert.is_equal(0, code)
    end

    -- Create cursor
    code, message, cursor = db:cursor_init(name)
    assert.is_equal(0, code)

    -- Test next operations
    for i=1,5 do
      code, message, key, value = cursor:get()
      assert.is_equal(0, code)
      assert.is_equal(key, "key" .. i)
      assert.is_equal(value, "value" .. i)
      code, message = cursor:next()
      if i ~= 5 then
        assert.is_equal(0, code)
      end
    end

    -- Position cursor at last element
    code, message = cursor:prev()
    assert.is_equal(0, code)

    -- Test prev operations
    for i=5,1,-1 do
      code, message, key, value = cursor:get()
      assert.is_equal(0, code)
      assert.is_equal(key, "key" .. i)
      assert.is_equal(value, "value" .. i)
      code, message = cursor:prev()
      if i ~= 1 then
        assert.is_equal(0, code)
      end
    end

    -- Free cursor
    code, message = cursor:free()
    assert.is_equal(0, code)

    code, message = db:drop_column_family(name)
    assert.is_equal(0, code)

    local code, message, db = lib.close(db)
    assert.is_equal(0, code)
  end)
end)
