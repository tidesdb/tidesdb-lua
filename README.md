# tidesdb-lua
Official Lua binding for TidesDB

[![Linux Build Status](https://github.com/tidesdb/tidesdb-lua/actions/workflows/build_and_test_lua.yml/badge.svg)](https://github.com/tidesdb/tidesdb-lua/actions/workflows/build_and_test_lua.yml)

#### Setup

This is a Lua wrapper library for TidesDB therefore first you need
a copy of TidesDB

```bash
git clone https://github.com/tidesdb/tidesdb.git

```
Build it and install
```bash
cd tidesdb
cmake -DTIDESDB_WITH_SANITIZER=OFF -S . -B build && make -C build/
sudo cmake --install build
```

Build Lua library
```bash
git clone https://github.com/tidesdb/tidesdb-lua.git
cd tidesdb-lua
cmake -S . -B build && make -C build/
```
As a result libtidesdb_lua.so library is built
#### Basic operations

```lua
-- Open lua wrapper library
local lib = require("libtidesdb_lua")

-- Open a TidesDB database
local code, message, db = lib.open("my_db")
--assert error codes for failures
assert(code == 0, message)

-- Create a column family
code, message = db:create_column_family(
    "my_column_family", 
    1024*1024*64,    -- Flush threshold (64MB)
    12,              -- Max level skip list, if using hash table is irrelevant
    0.24,            -- Probability skip list, if using hash table is irrelevant
    true,            -- Enable compression
    db.COMPRESS_SNAPPY, -- Compression algorithm can be NO_COMPRESSION, COMPRESS_SNAPPY, COMPRESS_LZ4, COMPRESS_ZSTD
    true,               -- Enable bloom filter
)

-- Put key-value pair into the database
code, message = db:put("my_column_family", "key", "value", 3600)

-- Get the value for the key
code, message, value = db:get("my_column_family", "key")

-- Delete the key-value pair
db:delete("my_column_family", "key")

--- Close the database
lib.close(db)
```

#### Test lua wrapper-library
```bash
cd build
lua ../test_lua.lua
```
