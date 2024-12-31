/*
 *
 * Copyright (C) Evgeny Kornev
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
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdio.h>
#include <string.h>
#include <tidesdb.h>

static tidesdb_t *db = NULL;

#define LUA_RET_CODE()                     \
    if(ret)                                \
    {                                      \
        lua_pushinteger(L, ret->code);     \
        lua_pushstring(L, ret->message);   \
        return 2;                          \
    } else {                               \
        lua_pushinteger(L, 0);             \
        lua_pushstring(L, "OK");           \
        return 2;                          \
    }                                      \

#define LUA_RET_CODE_AND_VALUE(_value, value_size)     \
    if(ret)                                            \
    {                                                  \
        lua_pushinteger(L, ret->code);                 \
        lua_pushstring(L, ret->message);               \
        return 2;                                      \
    } else {                                           \
        lua_pushinteger(L, 0);                         \
        lua_pushstring(L, "OK");                       \
        lua_pushlstring(L, _value, value_size);        \
        return 3;                                      \
    }                                                  \

static int db_open(lua_State *L)
{
    const char* directory = luaL_checkstring(L, 1);
    tidesdb_err_t *ret = tidesdb_open(directory, &db);
    LUA_RET_CODE()
}

static int db_close(lua_State *L)
{
    tidesdb_err_t *ret = tidesdb_close(db);
    LUA_RET_CODE()   
}

static int create_column_family(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    const int flush_threshold = luaL_checkinteger(L, 2);
    const int max_skip_level = luaL_checkinteger(L, 3);
    const float prob_skip_level = luaL_checknumber(L, 4);
    const bool enable_compression = lua_toboolean(L, 5);
    const tidesdb_compression_algo_t compression_algo = (tidesdb_compression_algo_t)luaL_checkinteger(L, 6);
    const bool enable_bloom_filter = lua_toboolean(L, 7);
    const tidesdb_memtable_ds_t db_data_struct = (tidesdb_memtable_ds_t)luaL_checkinteger(L, 8);
    tidesdb_err_t *ret = tidesdb_create_column_family(db,
                                                      column_family,
                                                      flush_threshold,
                                                      max_skip_level,
                                                      prob_skip_level,
                                                      enable_compression,
                                                      compression_algo,
                                                      enable_bloom_filter,
                                                      db_data_struct);
    LUA_RET_CODE()  
}
static int drop_column_family(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    tidesdb_err_t *ret = tidesdb_drop_column_family(db, column_family);
    LUA_RET_CODE()
}

static int put(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 2);
    const size_t key_size = (size_t)luaL_len(L, 2);
    const uint8_t* value = (uint8_t*)luaL_checkstring(L, 3);
    const size_t value_size = (size_t)luaL_len(L, 3);
    const int ttl = luaL_checkinteger(L, 4);
    tidesdb_err_t *ret = tidesdb_put(db,
                                     column_family,
                                     key,
                                     key_size,
                                     value,
                                     value_size,
                                     ttl == -1 ? ttl : ttl + time(NULL));
    LUA_RET_CODE()
}

static int get(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 2);
    const size_t key_size = (size_t)luaL_len(L, 2);
    uint8_t* value = NULL;
    size_t value_size = 0;
    tidesdb_err_t *ret = tidesdb_get(db,
                                     column_family,
                                     key,
                                     key_size,
                                     &value,
                                     &value_size);
    LUA_RET_CODE_AND_VALUE((char*)value, value_size)
}

static int delete(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 2);
    const size_t key_size = (size_t)luaL_len(L, 2);
    tidesdb_err_t *ret = tidesdb_delete(db,
                                        column_family,
                                        key,
                                        key_size);
    LUA_RET_CODE()
}

static int compact_sstables(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 1);
    const int max_threads = (uint32_t)luaL_checkinteger(L, 2);
    tidesdb_err_t *ret = tidesdb_compact_sstables(db,
                                        column_family,
                                        max_threads);
    LUA_RET_CODE()
}

static int list_column_families(lua_State *L)
{
    const char* list = tidesdb_list_column_families(db);
    lua_pushstring(L, list);
    return 1;
}


static const luaL_Reg regs_tidesdb_lua[] = {
    {"open", db_open},
    {"close", db_close},
    {"create_column_family", create_column_family},
    {"drop_column_family", drop_column_family},
    {"put", put},
    {"get", get},
    {"delete", delete},
    {"compact_sstables", compact_sstables},
    {"list_column_families", list_column_families},
    {NULL, NULL}
};

LUALIB_API int luaopen_libtidesdb_lua(lua_State *L)
{
    luaL_newlib(L, regs_tidesdb_lua);

    lua_pushnumber(L, 0);
    lua_setfield(L, -2, "NO_COMPRESSION");
    lua_pushnumber(L, 1);
    lua_setfield(L, -2, "COMPRESS_SNAPPY");
    lua_pushnumber(L, 2);
    lua_setfield(L, -2, "COMPRESS_LZ4");
    lua_pushnumber(L, 3);
    lua_setfield(L, -2, "COMPRESS_ZSTD");

    lua_pushnumber(L, 0);
    lua_setfield(L, -2, "TDB_MEMTABLE_SKIP_LIST");
    lua_pushnumber(L, 1);
    lua_setfield(L, -2, "TDB_MEMTABLE_HASH_TABLE");
    return 1;
}
