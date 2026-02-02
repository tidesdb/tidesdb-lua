package = "tidesdb"
version = "0.4.0-1"
source = {
   url = "git://github.com/tidesdb/tidesdb-lua.git",
   tag = "v0.4.0"
}
description = {
   summary = "Official Lua bindings for TidesDB - A high-performance embedded key-value storage engine",
   detailed = [[
      TidesDB is a fast and efficient key-value storage engine library written in C.
      The underlying data structure is based on a log-structured merge-tree (LSM-tree).
      This Lua binding provides a safe, idiomatic Lua interface to TidesDB with full
      support for all features including MVCC transactions, column families, iterators,
      TTL, compression, and bloom filters.
   ]],
   homepage = "https://tidesdb.com",
   license = "MPL-2.0",
   maintainer = "TidesDB Lead/Creator <me@alexpadula.com>"
}
dependencies = {
   "lua >= 5.1",
   "luajit >= 2.0"
}
build = {
   type = "builtin",
   modules = {
      tidesdb = "src/tidesdb.lua"
   }
}
