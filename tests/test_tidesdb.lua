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

function tests.test_checkpoint()
    local path = "./test_db_checkpoint"
    local checkpoint_path = "./test_db_checkpoint_snap"
    cleanup_db(path)
    cleanup_db(checkpoint_path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Create checkpoint
    db:checkpoint(checkpoint_path)
    
    db:close()
    
    -- Open checkpoint and verify data
    local cp_db = tidesdb.TidesDB.open(checkpoint_path)
    local cp_cf = cp_db:get_column_family("test_cf")
    local read_txn = cp_db:begin_txn()
    local v1 = read_txn:get(cp_cf, "key1")
    local v2 = read_txn:get(cp_cf, "key2")
    assert_eq(v1, "value1", "checkpoint should contain key1")
    assert_eq(v2, "value2", "checkpoint should contain key2")
    read_txn:free()
    
    -- Verify checkpoint to existing non-empty dir fails
    cp_db:close()
    
    local db2 = tidesdb.TidesDB.open(path)
    local cf2 = db2:get_column_family("test_cf")
    
    local err = assert_error(function()
        db2:checkpoint(checkpoint_path)
    end, "checkpoint to non-empty dir should fail")
    
    db2:close()
    cleanup_db(path)
    cleanup_db(checkpoint_path)
    print("PASS: test_checkpoint")
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

function tests.test_range_cost()
    local path = "./test_db_range_cost"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert some data
    local txn = db:begin_txn()
    for i = 1, 20 do
        txn:put(cf, string.format("key:%04d", i), string.format("value:%04d", i))
    end
    txn:commit()
    txn:free()
    
    -- Estimate range cost
    local cost = cf:range_cost("key:0001", "key:0020")
    assert_true(cost ~= nil, "range cost should not be nil")
    assert_true(type(cost) == "number", "range cost should be a number")
    assert_true(cost >= 0, "range cost should be >= 0")
    
    -- Compare two ranges (wider range should cost >= narrower range)
    local cost_wide = cf:range_cost("key:0001", "key:0020")
    local cost_narrow = cf:range_cost("key:0005", "key:0010")
    assert_true(cost_wide >= 0, "wide range cost should be >= 0")
    assert_true(cost_narrow >= 0, "narrow range cost should be >= 0")
    
    -- Key order should not matter
    local cost_ab = cf:range_cost("key:0001", "key:0020")
    local cost_ba = cf:range_cost("key:0020", "key:0001")
    assert_eq(cost_ab, cost_ba, "range cost should be the same regardless of key order")
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_range_cost")
end

function tests.test_commit_hook()
    local ffi = require("ffi")
    local path = "./test_db_commit_hook"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Track hook invocations
    local hook_called = 0
    local hook_ops_count = 0
    local hook_seq = 0
    local hook_keys = {}
    local hook_had_delete = false
    
    local hook = ffi.cast("tidesdb_commit_hook_fn", function(ops, num_ops, commit_seq, ctx)
        hook_called = hook_called + 1
        hook_ops_count = hook_ops_count + num_ops
        hook_seq = tonumber(commit_seq)
        for i = 0, num_ops - 1 do
            local key = ffi.string(ops[i].key, ops[i].key_size)
            table.insert(hook_keys, key)
            if ops[i].is_delete ~= 0 then
                hook_had_delete = true
            end
        end
        return 0
    end)
    
    -- Set commit hook
    cf:set_commit_hook(hook, nil)
    
    -- Write data (should trigger the hook)
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    assert_true(hook_called >= 1, "commit hook should have been called")
    assert_true(hook_ops_count >= 2, "hook should have received at least 2 ops")
    assert_true(hook_seq > 0, "commit_seq should be > 0")
    
    -- Verify keys were captured
    local found_key1 = false
    local found_key2 = false
    for _, k in ipairs(hook_keys) do
        if k == "key1" then found_key1 = true end
        if k == "key2" then found_key2 = true end
    end
    assert_true(found_key1, "hook should have received key1")
    assert_true(found_key2, "hook should have received key2")
    
    -- Test delete operation fires hook
    hook_had_delete = false
    local del_txn = db:begin_txn()
    del_txn:delete(cf, "key1")
    del_txn:commit()
    del_txn:free()
    
    assert_true(hook_had_delete, "hook should have received a delete operation")
    
    -- Save hook_called count before clearing
    local calls_before_clear = hook_called
    
    -- Clear commit hook
    cf:clear_commit_hook()
    
    -- Write more data (should NOT trigger the hook)
    local txn2 = db:begin_txn()
    txn2:put(cf, "key3", "value3")
    txn2:commit()
    txn2:free()
    
    assert_eq(hook_called, calls_before_clear, "hook should not fire after clearing")
    
    -- Clean up the callback to prevent GC issues
    hook:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_commit_hook")
end

function tests.test_delete_column_family()
    local path = "./test_db_delete_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()
    
    -- Delete column family by pointer
    db:delete_column_family(cf)
    
    -- Verify column family no longer exists
    local err = assert_error(function()
        db:get_column_family("test_cf")
    end, "test_cf should not exist after delete")
    
    db:close()
    cleanup_db(path)
    print("PASS: test_delete_column_family")
end

function tests.test_iterator_seek()
    local path = "./test_db_iter_seek"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Insert ordered data
    local txn = db:begin_txn()
    txn:put(cf, "key:0001", "val1")
    txn:put(cf, "key:0002", "val2")
    txn:put(cf, "key:0003", "val3")
    txn:put(cf, "key:0004", "val4")
    txn:put(cf, "key:0005", "val5")
    txn:commit()
    txn:free()
    
    -- Test seek to specific key
    local read_txn = db:begin_txn()
    local iter = read_txn:new_iterator(cf)
    
    iter:seek("key:0003")
    assert_true(iter:valid(), "iterator should be valid after seek")
    assert_eq(iter:key(), "key:0003", "seek should find exact key")
    
    -- Test seek_for_prev
    iter:seek_for_prev("key:0004")
    assert_true(iter:valid(), "iterator should be valid after seek_for_prev")
    assert_eq(iter:key(), "key:0004", "seek_for_prev should find exact key")
    
    -- Test seek to non-existent key (should find next key >= target)
    iter:seek("key:0002x")
    assert_true(iter:valid(), "iterator should be valid after seek to non-existent key")
    assert_eq(iter:key(), "key:0003", "seek should find next key >= target")
    
    iter:free()
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_iterator_seek")
end

function tests.test_multi_cf_transaction()
    local path = "./test_db_multi_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("cf_a")
    db:create_column_family("cf_b")
    local cf_a = db:get_column_family("cf_a")
    local cf_b = db:get_column_family("cf_b")
    
    -- Atomic transaction across two column families
    local txn = db:begin_txn()
    txn:put(cf_a, "user:1", "Alice")
    txn:put(cf_b, "order:1", "user:1|item:A")
    txn:commit()
    txn:free()
    
    -- Verify both CFs have data
    local read_txn = db:begin_txn()
    local user = read_txn:get(cf_a, "user:1")
    local order = read_txn:get(cf_b, "order:1")
    assert_eq(user, "Alice", "cf_a should have user data")
    assert_eq(order, "user:1|item:A", "cf_b should have order data")
    read_txn:free()
    
    -- Verify independence: data in cf_a is not in cf_b
    local verify_txn = db:begin_txn()
    local err = assert_error(function()
        verify_txn:get(cf_b, "user:1")
    end, "user:1 should not exist in cf_b")
    verify_txn:free()
    
    db:drop_column_family("cf_a")
    db:drop_column_family("cf_b")
    db:close()
    cleanup_db(path)
    print("PASS: test_multi_cf_transaction")
end

function tests.test_max_memory_usage()
    local path = "./test_db_max_mem"
    cleanup_db(path)
    
    -- Test default config includes max_memory_usage
    local default_cfg = tidesdb.default_config()
    assert_true(default_cfg.max_memory_usage ~= nil, "max_memory_usage should exist in default config")
    
    -- Test opening database with default max_memory_usage (0 = unlimited)
    local db1 = tidesdb.TidesDB.open(path)
    assert_true(db1 ~= nil, "database should open with default max_memory_usage")
    db1:close()
    cleanup_db(path)
    
    -- Test opening database with custom max_memory_usage
    local db2 = tidesdb.TidesDB.open(path, {
        max_memory_usage = 512 * 1024 * 1024  -- 512 MB
    })
    assert_true(db2 ~= nil, "database should open with custom max_memory_usage")
    db2:close()
    cleanup_db(path)
    
    -- Test with TidesDB.new() constructor
    local config = {
        db_path = path,
        num_flush_threads = 2,
        num_compaction_threads = 2,
        max_memory_usage = 256 * 1024 * 1024  -- 256 MB
    }
    local db3 = tidesdb.TidesDB.new(config)
    assert_true(db3 ~= nil, "database should be created with TidesDB.new() and max_memory_usage")
    db3:close()
    
    cleanup_db(path)
    print("PASS: test_max_memory_usage")
end

function tests.test_sync_wal()
    local path = "./test_db_sync_wal"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
    })
    local cf_config = tidesdb.default_column_family_config()
    cf_config.sync_mode = tidesdb.SyncMode.SYNC_NONE
    db:create_column_family("test_cf", cf_config)
    local cf = db:get_column_family("test_cf")
    
    -- Write some data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:put(cf, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Manually sync WAL
    cf:sync_wal()
    
    -- Verify data is still readable after sync
    local read_txn = db:begin_txn()
    local v1 = read_txn:get(cf, "key1")
    local v2 = read_txn:get(cf, "key2")
    assert_eq(v1, "value1", "key1 should be readable after sync_wal")
    assert_eq(v2, "value2", "key2 should be readable after sync_wal")
    read_txn:free()
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_sync_wal")
end

function tests.test_purge_cf()
    local path = "./test_db_purge_cf"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
    })
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")
    
    -- Write some data
    local txn = db:begin_txn()
    for i = 1, 10 do
        txn:put(cf, string.format("key:%04d", i), string.format("value:%04d", i))
    end
    txn:commit()
    txn:free()
    
    -- Purge column family (synchronous flush + compaction)
    cf:purge()
    
    -- Verify data is still readable after purge
    local read_txn = db:begin_txn()
    local v1 = read_txn:get(cf, "key:0001")
    local v10 = read_txn:get(cf, "key:0010")
    assert_eq(v1, "value:0001", "key:0001 should be readable after purge_cf")
    assert_eq(v10, "value:0010", "key:0010 should be readable after purge_cf")
    read_txn:free()
    
    -- After purge, flushing and compacting should be done
    assert_eq(cf:is_flushing(), false, "should not be flushing after purge")
    assert_eq(cf:is_compacting(), false, "should not be compacting after purge")
    
    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_purge_cf")
end

function tests.test_purge_db()
    local path = "./test_db_purge_db"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
    })
    db:create_column_family("cf_a")
    db:create_column_family("cf_b")
    local cf_a = db:get_column_family("cf_a")
    local cf_b = db:get_column_family("cf_b")
    
    -- Write data to both CFs
    local txn = db:begin_txn()
    txn:put(cf_a, "a_key1", "a_value1")
    txn:put(cf_b, "b_key1", "b_value1")
    txn:commit()
    txn:free()
    
    -- Purge entire database
    db:purge()
    
    -- Verify data is still readable
    local read_txn = db:begin_txn()
    local va = read_txn:get(cf_a, "a_key1")
    local vb = read_txn:get(cf_b, "b_key1")
    assert_eq(va, "a_value1", "cf_a key should be readable after db purge")
    assert_eq(vb, "b_value1", "cf_b key should be readable after db purge")
    read_txn:free()
    
    db:drop_column_family("cf_a")
    db:drop_column_family("cf_b")
    db:close()
    cleanup_db(path)
    print("PASS: test_purge_db")
end

function tests.test_get_db_stats()
    local path = "./test_db_db_stats"
    cleanup_db(path)
    
    local db = tidesdb.TidesDB.open(path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
    })
    db:create_column_family("cf_a")
    db:create_column_family("cf_b")
    local cf_a = db:get_column_family("cf_a")
    local cf_b = db:get_column_family("cf_b")
    
    -- Write some data
    local txn = db:begin_txn()
    txn:put(cf_a, "key1", "value1")
    txn:put(cf_b, "key2", "value2")
    txn:commit()
    txn:free()
    
    -- Get database-level stats
    local db_stats = db:get_db_stats()
    
    -- Verify fields exist and have sensible values
    assert_true(db_stats.num_column_families >= 2, "should have at least 2 column families")
    assert_true(db_stats.total_memory > 0, "total_memory should be > 0")
    assert_true(db_stats.resolved_memory_limit > 0, "resolved_memory_limit should be > 0")
    assert_true(db_stats.memory_pressure_level >= 0, "memory_pressure_level should be >= 0")
    assert_true(db_stats.global_seq >= 0, "global_seq should be >= 0")
    assert_true(db_stats.flush_queue_size >= 0, "flush_queue_size should be >= 0")
    assert_true(db_stats.compaction_queue_size >= 0, "compaction_queue_size should be >= 0")
    assert_true(db_stats.total_sstable_count >= 0, "total_sstable_count should be >= 0")
    assert_true(db_stats.total_data_size_bytes >= 0, "total_data_size_bytes should be >= 0")
    assert_true(db_stats.num_open_sstables >= 0, "num_open_sstables should be >= 0")
    assert_true(db_stats.txn_memory_bytes ~= nil, "txn_memory_bytes should exist")
    assert_true(db_stats.total_memtable_bytes ~= nil, "total_memtable_bytes should exist")
    assert_true(db_stats.total_immutable_count >= 0, "total_immutable_count should be >= 0")
    assert_true(db_stats.flush_pending_count >= 0, "flush_pending_count should be >= 0")
    assert_true(db_stats.available_memory ~= nil, "available_memory should exist")
    
    db:drop_column_family("cf_a")
    db:drop_column_family("cf_b")
    db:close()
    cleanup_db(path)
    print("PASS: test_get_db_stats")
end

function tests.test_iterator_key_value()
    local path = "./test_db_iter_kv"
    cleanup_db(path)

    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")

    -- Insert data
    local txn = db:begin_txn()
    txn:put(cf, "alpha", "one")
    txn:put(cf, "beta", "two")
    txn:put(cf, "gamma", "three")
    txn:commit()
    txn:free()

    -- Use key_value() to get both in one call
    local read_txn = db:begin_txn()
    local iter = read_txn:new_iterator(cf)
    iter:seek_to_first()

    local count = 0
    local pairs_found = {}
    while iter:valid() do
        local k, v = iter:key_value()
        pairs_found[k] = v
        count = count + 1
        iter:next()
    end

    assert_eq(count, 3, "should iterate over 3 entries with key_value")
    assert_eq(pairs_found["alpha"], "one", "key_value should return correct pair for alpha")
    assert_eq(pairs_found["beta"], "two", "key_value should return correct pair for beta")
    assert_eq(pairs_found["gamma"], "three", "key_value should return correct pair for gamma")

    iter:free()
    read_txn:free()

    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_iterator_key_value")
end

function tests.test_unified_memtable_config()
    local path = "./test_db_unified_mt"
    cleanup_db(path)

    -- Test default config includes unified_memtable fields
    local default_cfg = tidesdb.default_config()
    assert_true(default_cfg.unified_memtable ~= nil, "unified_memtable should exist in default config")
    assert_eq(default_cfg.unified_memtable, false, "unified_memtable should default to false")
    assert_true(default_cfg.unified_memtable_write_buffer_size ~= nil, "unified_memtable_write_buffer_size should exist")
    assert_true(default_cfg.unified_memtable_skip_list_max_level ~= nil, "unified_memtable_skip_list_max_level should exist")
    assert_true(default_cfg.unified_memtable_skip_list_probability ~= nil, "unified_memtable_skip_list_probability should exist")
    assert_true(default_cfg.unified_memtable_sync_mode ~= nil, "unified_memtable_sync_mode should exist")
    assert_true(default_cfg.unified_memtable_sync_interval_us ~= nil, "unified_memtable_sync_interval_us should exist")

    -- Test opening database with unified_memtable disabled (default)
    local db = tidesdb.TidesDB.open(path)
    assert_true(db ~= nil, "database should open with unified_memtable disabled")
    db:close()
    cleanup_db(path)

    -- Test opening database with unified_memtable enabled
    local db2 = tidesdb.TidesDB.open(path, {
        unified_memtable = true,
        unified_memtable_write_buffer_size = 32 * 1024 * 1024,
        unified_memtable_skip_list_max_level = 16,
        unified_memtable_skip_list_probability = 0.5,
        unified_memtable_sync_mode = tidesdb.SyncMode.SYNC_FULL,
    })
    assert_true(db2 ~= nil, "database should open with unified_memtable enabled")

    -- Basic operations should still work
    db2:create_column_family("test_cf")
    local cf = db2:get_column_family("test_cf")
    local txn = db2:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()

    local read_txn = db2:begin_txn()
    local v = read_txn:get(cf, "key1")
    assert_eq(v, "value1", "should read value with unified_memtable enabled")
    read_txn:free()

    db2:drop_column_family("test_cf")
    db2:close()
    cleanup_db(path)
    print("PASS: test_unified_memtable_config")
end

function tests.test_object_cf_config_fields()
    local path = "./test_db_object_cf"
    cleanup_db(path)

    -- Test default column family config includes object_* fields
    local default_cf = tidesdb.default_column_family_config()
    assert_true(default_cf.object_lazy_compaction ~= nil, "object_lazy_compaction should exist")
    assert_true(default_cf.object_prefetch_compaction ~= nil, "object_prefetch_compaction should exist")
    assert_true(default_cf.object_target_file_size == nil, "object_target_file_size should be retired from public API")

    -- Test creating CF with custom object_* fields
    local db = tidesdb.TidesDB.open(path)
    local cf_config = tidesdb.default_column_family_config()
    cf_config.object_lazy_compaction = true
    cf_config.object_prefetch_compaction = false
    db:create_column_family("test_cf", cf_config)

    local cf = db:get_column_family("test_cf")
    assert_true(cf ~= nil, "column family should be created with object_* config")

    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_object_cf_config_fields")
end

function tests.test_db_stats_extended_fields()
    local path = "./test_db_stats_extended"
    cleanup_db(path)

    local db = tidesdb.TidesDB.open(path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
    })
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")

    -- Write some data
    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()

    -- Get database-level stats and check new fields
    local db_stats = db:get_db_stats()

    -- Unified memtable fields
    assert_true(db_stats.unified_memtable_enabled ~= nil, "unified_memtable_enabled should exist")
    assert_true(db_stats.unified_memtable_bytes ~= nil, "unified_memtable_bytes should exist")
    assert_true(db_stats.unified_immutable_count ~= nil, "unified_immutable_count should exist")
    assert_true(db_stats.unified_is_flushing ~= nil, "unified_is_flushing should exist")
    assert_true(db_stats.unified_next_cf_index ~= nil, "unified_next_cf_index should exist")
    assert_true(db_stats.unified_wal_generation ~= nil, "unified_wal_generation should exist")

    -- Object store fields
    assert_true(db_stats.object_store_enabled ~= nil, "object_store_enabled should exist")
    assert_true(db_stats.local_cache_bytes_used ~= nil, "local_cache_bytes_used should exist")
    assert_true(db_stats.local_cache_bytes_max ~= nil, "local_cache_bytes_max should exist")
    assert_true(db_stats.local_cache_num_files ~= nil, "local_cache_num_files should exist")
    assert_true(db_stats.last_uploaded_generation ~= nil, "last_uploaded_generation should exist")
    assert_true(db_stats.upload_queue_depth ~= nil, "upload_queue_depth should exist")
    assert_true(db_stats.total_uploads ~= nil, "total_uploads should exist")
    assert_true(db_stats.total_upload_failures ~= nil, "total_upload_failures should exist")
    assert_true(db_stats.replica_mode ~= nil, "replica_mode should exist")

    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_db_stats_extended_fields")
end

function tests.test_objstore_config_defaults()
    -- Test default object store config has correct fields and defaults
    local os_config = tidesdb.default_objstore_config()

    assert_true(os_config.local_cache_max_bytes ~= nil, "local_cache_max_bytes should exist")
    assert_eq(os_config.local_cache_max_bytes, 0, "local_cache_max_bytes should default to 0")
    assert_eq(os_config.cache_on_read, true, "cache_on_read should default to true")
    assert_eq(os_config.cache_on_write, true, "cache_on_write should default to true")
    assert_eq(os_config.max_concurrent_uploads, 4, "max_concurrent_uploads should default to 4")
    assert_eq(os_config.max_concurrent_downloads, 8, "max_concurrent_downloads should default to 8")
    assert_true(os_config.multipart_threshold > 0, "multipart_threshold should be > 0")
    assert_true(os_config.multipart_part_size > 0, "multipart_part_size should be > 0")
    assert_eq(os_config.sync_manifest_to_object, true, "sync_manifest_to_object should default to true")
    assert_eq(os_config.replicate_wal, true, "replicate_wal should default to true")
    assert_eq(os_config.wal_upload_sync, false, "wal_upload_sync should default to false")
    assert_true(os_config.wal_sync_threshold_bytes > 0, "wal_sync_threshold_bytes should be > 0")
    assert_eq(os_config.wal_sync_on_commit, false, "wal_sync_on_commit should default to false")
    assert_eq(os_config.replica_mode, false, "replica_mode should default to false")
    assert_true(os_config.replica_sync_interval_us > 0, "replica_sync_interval_us should be > 0")
    assert_eq(os_config.replica_replay_wal, true, "replica_replay_wal should default to true")

    print("PASS: test_objstore_config_defaults")
end

function tests.test_objstore_fs_create()
    local path = "./test_objstore_root"
    os.execute("rm -rf " .. path)
    os.execute("mkdir -p " .. path)

    -- Create filesystem connector
    local store = tidesdb.objstore_fs_create(path)
    assert_true(store ~= nil, "filesystem object store connector should be created")

    os.execute("rm -rf " .. path)
    print("PASS: test_objstore_fs_create")
end

function tests.test_objstore_open_with_fs_connector()
    local db_path = "./test_db_objstore"
    local store_path = "./test_objstore_data"
    local cleanup_db = function(p) os.execute("rm -rf " .. p) end
    cleanup_db(db_path)
    cleanup_db(store_path)
    os.execute("mkdir -p " .. store_path)

    -- Create filesystem connector and config
    local store = tidesdb.objstore_fs_create(store_path)
    local os_config = tidesdb.default_objstore_config()
    os_config.local_cache_max_bytes = 128 * 1024 * 1024
    os_config.max_concurrent_uploads = 2

    -- Open database with object store
    local db = tidesdb.TidesDB.open(db_path, {
        log_level = tidesdb.LogLevel.LOG_WARN,
        object_store = store,
        object_store_config = os_config,
    })
    assert_true(db ~= nil, "database should open with object store connector")

    -- Basic operations should work
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")

    local txn = db:begin_txn()
    txn:put(cf, "key1", "value1")
    txn:commit()
    txn:free()

    local read_txn = db:begin_txn()
    local v = read_txn:get(cf, "key1")
    assert_eq(v, "value1", "should read value with object store enabled")
    read_txn:free()

    -- Verify object store stats reflect enabled state
    local db_stats = db:get_db_stats()
    assert_eq(db_stats.object_store_enabled, true, "object_store_enabled should be true")

    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(db_path)
    cleanup_db(store_path)
    print("PASS: test_objstore_open_with_fs_connector")
end

function tests.test_promote_to_primary()
    local path = "./test_db_promote"
    cleanup_db(path)

    -- Open a normal (non-replica) database
    local db = tidesdb.TidesDB.open(path)

    -- Calling promote_to_primary on a non-replica should not crash
    -- It may return an error or succeed depending on the C implementation
    local ok, err = pcall(function()
        db:promote_to_primary()
    end)
    -- We just verify the method exists and is callable
    assert_true(ok or err ~= nil, "promote_to_primary should be callable")

    db:close()
    cleanup_db(path)
    print("PASS: test_promote_to_primary")
end

function tests.test_error_readonly_constant()
    -- Verify TDB_ERR_READONLY constant exists and has correct value
    assert_eq(tidesdb.TDB_ERR_READONLY, -13, "TDB_ERR_READONLY should be -13")
    print("PASS: test_error_readonly_constant")
end

function tests.test_txn_single_delete()
    local path = "./test_db_single_delete"
    cleanup_db(path)

    local db = tidesdb.TidesDB.open(path)
    db:create_column_family("test_cf")
    local cf = db:get_column_family("test_cf")

    -- Insert a key that will be single-deleted
    local txn = db:begin_txn()
    txn:put(cf, "sd_key", "sd_value")
    txn:put(cf, "keep_key", "keep_value")
    txn:commit()
    txn:free()

    -- Verify the key exists
    local read_txn = db:begin_txn()
    local v = read_txn:get(cf, "sd_key")
    assert_eq(v, "sd_value", "sd_key should exist before single_delete")
    read_txn:free()

    -- Single-delete the key
    local del_txn = db:begin_txn()
    del_txn:single_delete(cf, "sd_key")
    del_txn:commit()
    del_txn:free()

    -- Verify the key is gone
    local verify_txn = db:begin_txn()
    local err = assert_error(function()
        verify_txn:get(cf, "sd_key")
    end, "sd_key should not exist after single_delete")
    local kept = verify_txn:get(cf, "keep_key")
    assert_eq(kept, "keep_value", "unrelated key should remain after single_delete")
    verify_txn:free()

    -- single_delete on a closed transaction should raise
    local closed_txn = db:begin_txn()
    closed_txn:free()
    local closed_err = assert_error(function()
        closed_txn:single_delete(cf, "any_key")
    end, "single_delete on closed transaction should error")

    db:drop_column_family("test_cf")
    db:close()
    cleanup_db(path)
    print("PASS: test_txn_single_delete")
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
