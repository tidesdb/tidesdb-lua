--[[
TidesDB Lua Bindings Tests

Copyright (C) TidesDB
Licensed under the Mozilla Public License, v. 2.0
]]

local tidesdb = require("tidesdb")

-- Test utilities
local function assert_eq(a, b, msg)
    if a ~= b then
        error(string.format("%s: expected %s, got %s", msg or "assertion failed", tostring(b), tostring(a)))
    end
end

local function assert_true(cond, msg)
    if not cond then
        error(msg or "assertion failed: expected true")
    end
end

local function assert_error(fn, msg)
    local ok, err = pcall(fn)
    if ok then
        error(msg or "expected error but none was raised")
    end
    return err
end

local function cleanup_db(path)
    os.execute("rm -rf " .. path)
end

-- Test cases
local tests = {}

function tests.test_open_close()
    local path = "./test_db_open_close"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    assert_true(db ~= nil, "database should be opened")
    db:close()
    
    cleanup_db(path)
    print("PASS: test_open_close")
end

function tests.test_column_family_operations()
    local path = "./test_db_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    
    -- Create column family
    local cf_config = tidesdb.default_column_family_config()
    cf_config.compression_algorithm = tidesdb.CompressionAlgorithm.LZ4_COMPRESSION
    db:create_column_family("test_cf", cf_config)
    
    -- Get column family
    local cf = db:get_column_family("test_cf")
    assert_eq(cf.name, "test_cf", "column family name")
    
    -- List column families
    local cfs = db:list_column_families()
    assert_true(#cfs >= 1, "should have at least one column family")
    
    -- Drop column family
    db:drop_column_family("test_cf")
    
    db:close()
    cleanup_db(path)
    print("PASS: test_column_family_operations")
end

function tests.test_put_get_delete()
    local path = "./test_db_crud"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Put
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Get
    local read_txn = db:begin_txn()
    local value1 = read_txn:get(cf, "key1")
    local value2 = read_txn:get(cf, "key2")
    assert_eq(value1, "value1", "get key1")
    assert_eq(value2, "value2", "get key2")
    read_txn:free()
    
    -- Delete
    local del_txn = db:begin_txn()
    del_txn:delete(cf, "key1")
    del_txn:commit()
    del_txn:free()
    
    -- Verify deletion
    local verify_txn = db:begin_txn()
    local err = assert_error(function()
        verify_txn:get(cf, "key1")
    end, "should error on deleted key")
    verify_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_put_get_delete")
end

function tests.test_transaction_rollback()
    local path = "./test_db_rollback"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Put initial value
    local txn1 = db:begin_txn()
    txn1:put(cf, "key1", "initial")
    txn1:commit()
    txn1:free()
    
    -- Start transaction and rollback
    local txn2 = db:begin_txn()
    txn2:put(cf, "key1", "modified")
    txn2:rollback()
    txn2:free()
    
    -- Verify value unchanged
    local read_txn = db:begin_txn()
    local value = read_txn:get(cf, "key1")
    assert_eq(value, "initial", "value should be unchanged after rollback")
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_transaction_rollback")
end

function tests.test_iterator()
    local path = "./test_db_iter"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "a", "1")
    txn:put(cf, "b", "2")
    txn:put(cf, "c", "3")
    txn:commit()
    txn:free()
    
    -- Forward iteration
    local read_txn = db:begin_txn()
    local iter = read_txn:new_iterator(cf)
    iter:seek_to_first()
    
    local count = 0
    while iter:valid() do
        local key = iter:key()
        local value = iter:value()
        count = count + 1
        iter:next()
    end
    assert_eq(count, 3, "should iterate over 3 entries")
    
    iter:free()
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_iterator")
end

function tests.test_savepoints()
    local path = "./test_db_savepoint"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    
    -- Create savepoint
    txn:savepoint("sp1")
    txn:put(cf, "key2", "value2")
    
    -- Rollback to savepoint
    txn:rollback_to_savepoint("sp1")
    
    -- Commit (only key1 should be written)
    txn:commit()
    txn:free()
    
    -- Verify
    local read_txn = db:begin_txn()
    local value1 = read_txn:get(cf, "key1")
    assert_eq(value1, "value1", "key1 should exist")
    
    local err = assert_error(function()
        read_txn:get(cf, "key2")
    end, "key2 should not exist")
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_savepoints")
end

function tests.test_isolation_levels()
    local path = "./test_db_isolation"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Test different isolation levels
    local txn1 = db:begin_txn_with_isolation(tidesdb.IsolationLevel.READ_COMMITTED)
    txn1:put(cf, "key1", "value1")
    txn1:commit()
    txn1:free()
    
    local txn2 = db:begin_txn_with_isolation(tidesdb.IsolationLevel.SERIALIZABLE)
    local value = txn2:get(cf, "key1")
    assert_eq(value, "value1", "should read committed value")
    txn2:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_isolation_levels")
end

function tests.test_stats()
    local path = "./test_db_stats"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert some data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()
    
    -- Get stats
    local stats = cf:get_stats()
    assert_true(stats.num_levels >= 0, "num_levels should be >= 0")
    assert_true(stats.memtable_size >= 0, "memtable_size should be >= 0")
    -- Test new stats fields
    assert_true(stats.total_keys ~= nil, "total_keys should exist")
    assert_true(stats.total_data_size ~= nil, "total_data_size should exist")
    assert_true(stats.avg_key_size ~= nil, "avg_key_size should exist")
    assert_true(stats.avg_value_size ~= nil, "avg_value_size should exist")
    assert_true(stats.read_amp ~= nil, "read_amp should exist")
    assert_true(stats.hit_rate ~= nil, "hit_rate should exist")
    
    -- Get cache stats
    local cache_stats = db:get_cache_stats()
    assert_true(cache_stats.total_entries >= 0, "total_entries should be >= 0")
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_stats")
end

function tests.test_rename_column_family()
    local path = "./test_db_rename_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("old_cf")
    
    -- Insert data
    local cf = db:get_column_family("old_cf")
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()
    
    -- Rename column family
    db:rename_column_family("old_cf", "new_cf")
    
    -- Verify old name doesn't exist
    local err = assert_error(function()
        db:get_column_family("old_cf")
    end, "old_cf should not exist after rename")
    
    -- Verify new name exists and data is preserved
    local new_cf = db:get_column_family("new_cf")
    local read_txn = db:begin_txn()
    local value = read_txn:get(new_cf, "key1")
    assert_eq(value, "value1", "data should be preserved after rename")
    read_txn:free()
    
    db:drop_column_family("new_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_rename_column_family")
end

function tests.test_is_flushing_compacting()
    local path = "./test_db_flush_compact"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Check status (should be false when idle)
    local is_flushing = cf:is_flushing()
    local is_compacting = cf:is_compacting()
    assert_true(is_flushing == false or is_flushing == true, "is_flushing should return boolean")
    assert_true(is_compacting == false or is_compacting == true, "is_compacting should return boolean")
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_is_flushing_compacting")
end

function tests.test_backup()
    local path = "./test_db_backup"
    local backup_path = "./test_db_backup_copy"
    cleanup_db(path)
    cleanup_db(backup_path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()
    
    -- Create backup
    db:backup(backup_path)
    
    db:close()
    
    -- Open backup and verify data
    local backup_db = tidesdb.TidesDB.open(backup_path)
    local backup_cf = backup_db:get_column_family("test_cf")
    local read_txn = backup_db:begin_txn()
    local value = read_txn:get(backup_cf, "key1")
    assert_eq(value, "value1", "backup should contain original data")
    read_txn:free()
    
    backup_db:close()
    cleanup_db(path)
    cleanup_db(backup_path)
    print("PASS: test_backup")
end

function tests.test_update_runtime_config()
    local path = "./test_db_runtime_config"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Get current config
    local stats = cf:get_stats()
    local original_write_buffer_size = stats.config.write_buffer_size
    
    -- Update runtime config
    local new_config = tidesdb.default_column_family_config()
    new_config.write_buffer_size = 128 * 1024 * 1024  -- 128MB
    cf:update_runtime_config(new_config, false)  -- don't persist
    
    -- Verify config was updated
    local new_stats = cf:get_stats()
    assert_eq(new_stats.config.write_buffer_size, 128 * 1024 * 1024, "write_buffer_size should be updated")
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_update_runtime_config")
end

function tests.test_use_btree_config()
    local path = "./test_db_btree"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    
    -- Create column family with use_btree enabled
    local cf_config = tidesdb.default_column_family_config()
    cf_config.use_btree = true
    db:create_column_family("btree_cf", cf_config)
    
    local cf = db:get_column_family("btree_cf")
    
    -- Insert some data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Verify use_btree in stats
    local stats = cf:get_stats()
    assert_true(stats.use_btree ~= nil, "use_btree should exist in stats")
    assert_true(stats.config.use_btree ~= nil, "use_btree should exist in config")
    
    -- Verify B+tree stats fields exist
    assert_true(stats.btree_total_nodes ~= nil, "btree_total_nodes should exist")
    assert_true(stats.btree_max_height ~= nil, "btree_max_height should exist")
    assert_true(stats.btree_avg_height ~= nil, "btree_avg_height should exist")
    
    -- Read back data to verify it works
    local read_txn = db:begin_txn()
    local value1 = read_txn:get(cf, "key1")
    local value2 = read_txn:get(cf, "key2")
    assert_eq(value1, "value1", "get key1 with btree")
    assert_eq(value2, "value2", "get key2 with btree")
    read_txn:free()
    
    db:drop_column_family("btree_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_use_btree_config")
end

function tests.test_btree_stats_extended()
    local path = "./test_db_btree_stats"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    
    -- Create column family without btree (default)
    local cf_config = tidesdb.default_column_family_config()
    cf_config.use_btree = false
    db:create_column_family("block_cf", cf_config)
    
    local cf = db:get_column_family("block_cf")
    
    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()
    
    -- Verify stats
    local stats = cf:get_stats()
    assert_eq(stats.use_btree, false, "use_btree should be false for block-based CF")
    assert_eq(stats.config.use_btree, false, "config.use_btree should be false")
    
    -- B+tree stats should still exist but be zero/default for non-btree CF
    assert_true(stats.btree_total_nodes ~= nil, "btree_total_nodes should exist")
    assert_true(stats.btree_max_height ~= nil, "btree_max_height should exist")
    assert_true(stats.btree_avg_height ~= nil, "btree_avg_height should exist")
    
    db:drop_column_family("block_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_btree_stats_extended")
end

function tests.test_clone_column_family()
    local path = "./test_db_clone_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("source_cf")
    local cf = db:get_column_family("source_cf")
    
    -- Insert data into source
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Clone column family
    db:clone_column_family("source_cf", "cloned_cf")
    
    -- Verify cloned column family exists
    local cloned_cf = db:get_column_family("cloned_cf")
    assert_true(cloned_cf ~= nil, "cloned column family should exist")
    
    -- Verify data is preserved in clone
    local read_txn = db:begin_txn()
    local v1 = read_txn:get(cloned_cf, "key1")
    local v2 = read_txn:get(cloned_cf, "key2")
    assert_eq(v1, "value1", "cloned key1 should have correct value")
    assert_eq(v2, "value2", "cloned key2 should have correct value")
    read_txn:free()
    
    -- Verify source still works independently
    local src_txn = db:begin_txn()
    local sv1 = src_txn:get(cf, "key1")
    assert_eq(sv1, "value1", "source key1 should still exist")
    src_txn:free()
    
    -- Verify modifications to clone don't affect source
    local write_txn = db:begin_txn()
    write_txn:put(cloned_cf, "key3", "value3")
    write_txn:commit()
    write_txn:free()
    
    local verify_txn = db:begin_txn()
    local v3 = verify_txn:get(cloned_cf, "key3")
    assert_eq(v3, "value3", "key3 should exist in clone")
    local err = assert_error(function()
        verify_txn:get(cf, "key3")
    end, "key3 should not exist in source")
    verify_txn:free()
    
    -- Verify cloning to existing name fails
    local clone_err = assert_error(function()
        db:clone_column_family("source_cf", "cloned_cf")
    end, "cloning to existing name should fail")
    
    db:drop_column_family("source_cf")
    db:drop_column_family("cloned_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_clone_column_family")
end

function tests.test_transaction_reset()
    local path = "./test_db_txn_reset"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Begin transaction and do first batch of work
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    
    -- Reset transaction instead of free + begin
    txn:reset(tidesdb.IsolationLevel.READ_COMMITTED)
    
    -- Second batch of work using the same transaction
    txn:put(cf, "key2", "value2")
    txn:commit()
    
    -- Reset again with different isolation level
    txn:reset(tidesdb.IsolationLevel.SERIALIZABLE)
    
    -- Third batch
    txn:put(cf, "key3", "value3")
    txn:commit()
    
    txn:free()
    
    -- Verify all data was written
    local read_txn = db:begin_txn()
    local v1 = read_txn:get(cf, "key1")
    local v2 = read_txn:get(cf, "key2")
    local v3 = read_txn:get(cf, "key3")
    assert_eq(v1, "value1", "key1 should exist after reset")
    assert_eq(v2, "value2", "key2 should exist after reset")
    assert_eq(v3, "value3", "key3 should exist after reset")
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_transaction_reset")
end

-- Run all tests
local function run_tests()
    print("Running TidesDB Lua tests...")
    print("")
    
    local passed = 0
    local failed = 0
    
    for name, test_fn in pairs(tests) do
        local ok, err = pcall(test_fn)
        if ok then
            passed = passed + 1
        else
            failed = failed + 1
            print(string.format("FAIL: %s - %s", name, tostring(err)))
        end
    end
    
    print("")
    print(string.format("Results: %d passed, %d failed", passed, failed))
    
    if failed > 0 then
        os.exit(1)
    end
end

run_tests()
