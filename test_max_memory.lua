--[[
Test script to verify max_memory_usage field addition to tidesdb_config_t
This tests that the new field can be set and the database opens successfully.
]]

local tidesdb = require("tidesdb")

print("=== Testing max_memory_usage field ===\n")

-- Test 1: Default config includes max_memory_usage
print("Test 1: Checking default_config()...")
local default_cfg = tidesdb.default_config()
assert(default_cfg.max_memory_usage ~= nil, "max_memory_usage should exist in default config")
print("✓ max_memory_usage found in default config: " .. tostring(default_cfg.max_memory_usage))

-- Test 2: Open database with default max_memory_usage (0 = unlimited)
print("\nTest 2: Opening database with default max_memory_usage...")
local path1 = "./test_db_max_mem_default"
os.execute("rm -rf " .. path1)
local db1 = tidesdb.TidesDB.open(path1)
print("✓ Database opened successfully with default max_memory_usage")
db1:close()
os.execute("rm -rf " .. path1)

-- Test 3: Open database with custom max_memory_usage
print("\nTest 3: Opening database with custom max_memory_usage...")
local path2 = "./test_db_max_mem_custom"
os.execute("rm -rf " .. path2)
local db2 = tidesdb.TidesDB.open(path2, {
    max_memory_usage = 512 * 1024 * 1024  -- 512 MB
})
print("✓ Database opened successfully with max_memory_usage = 512 MB")
db2:close()
os.execute("rm -rf " .. path2)

-- Test 4: Test with TidesDB.new() constructor
print("\nTest 4: Testing with TidesDB.new() constructor...")
local path3 = "./test_db_max_mem_new"
os.execute("rm -rf " .. path3)
local config = {
    db_path = path3,
    num_flush_threads = 2,
    num_compaction_threads = 2,
    max_memory_usage = 256 * 1024 * 1024  -- 256 MB
}
local db3 = tidesdb.TidesDB.new(config)
print("✓ Database created with TidesDB.new() with max_memory_usage = 256 MB")
db3:close()
os.execute("rm -rf " .. path3)

print("\n=== All tests passed! ===")
print("\nThe max_memory_usage field has been successfully added to tidesdb_config_t.")
print("You can now configure memory limits when opening a TidesDB database.")
