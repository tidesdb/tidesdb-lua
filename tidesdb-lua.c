/*
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
 */
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdio.h>
#include <string.h>
#include <tidesdb/tidesdb.h>

#define LUA_RET_CODE()                     \
    if(ret)                                \
    {                                      \
        lua_pushinteger(L, ret->code);     \
        lua_pushstring(L, ret->message);   \
        tidesdb_err_free(ret);             \
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
        tidesdb_err_free(ret);                         \
        return 2;                                      \
    } else {                                           \
        lua_pushinteger(L, 0);                         \
        lua_pushstring(L, "OK");                       \
        lua_pushlstring(L, _value, value_size);        \
        free(_value);                                  \
        return 3;                                      \
    }                                                  \


static int db_open(lua_State *L);
static int db_close(lua_State *L);
static int create_column_family(lua_State *L);
static int drop_column_family(lua_State *L);
static int put(lua_State *L);
static int get(lua_State *L);
static int delete(lua_State *L);
static int list_column_families(lua_State *L);
static int compact_sstables(lua_State *L);

static int txn_begin(lua_State *L);
static int txn_put(lua_State *L);
static int txn_delete(lua_State *L);
static int txn_commit(lua_State *L);
static int txn_rollback(lua_State *L);
static int txn_free(lua_State *L);

static const luaL_Reg regs_tidesdb_lib_lua[] = {
    {"open", db_open},
    {"close", db_close},
    {"txn_begin", txn_begin},
    {NULL, NULL},
};

static const luaL_Reg regs_tidesdb_lua[] = {
    {"create_column_family", create_column_family},
    {"drop_column_family", drop_column_family},
    {"put", put},
    {"get", get},
    {"delete", delete},
    {"compact_sstables", compact_sstables},
    {"list_column_families", list_column_families},
    {"txn_begin", txn_begin},
    {NULL, NULL},
};

static const luaL_Reg regs_tidesdb_txn_lua[] = {
    {"put", txn_put},
    {"delete", txn_delete},
    {"commit", txn_commit},
    {"rollback", txn_rollback},
    {"free", txn_free},
    {NULL, NULL},
};

static int db_open(lua_State *L)
{
    const char* directory = luaL_checkstring(L, 1);
    tidesdb_t *db = NULL;
    tidesdb_err_t *ret = tidesdb_open(directory, &db);
    if(ret) {
        lua_pushinteger(L, ret->code);
        lua_pushstring(L, ret->message);
        tidesdb_err_free(ret);
        return 2;
    } else {

        lua_pushinteger(L, 0);
        lua_pushstring(L, "OK");

        luaL_newlib(L, regs_tidesdb_lua);
        lua_pushlightuserdata(L, db);
        lua_setfield(L, -2, "self_db");
        return 3;
    }
}

static int db_close(lua_State *L)
{
    lua_getfield(L, -1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_close(db);
    LUA_RET_CODE()   
}

static int create_column_family(lua_State *L)
{

    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    

    const char* column_family = luaL_checkstring(L, 2);
    const int flush_threshold = luaL_checkinteger(L, 3);
    const int max_skip_level = luaL_checkinteger(L, 4);
    const float prob_skip_level = luaL_checknumber(L, 5);
    const bool enable_compression = lua_toboolean(L, 6);
    const tidesdb_compression_algo_t compression_algo = (tidesdb_compression_algo_t)luaL_checkinteger(L, 7);
    const bool enable_bloom_filter = lua_toboolean(L, 8);
    const tidesdb_memtable_ds_t db_data_struct = (tidesdb_memtable_ds_t)luaL_checkinteger(L, 9);
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
    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    const char* column_family = luaL_checkstring(L, 2);
    tidesdb_err_t *ret = tidesdb_drop_column_family(db, column_family);
    LUA_RET_CODE()
}

static int put(lua_State *L)
{
    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);

    const char* column_family = luaL_checkstring(L, 2);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 3);
    const size_t key_size = (size_t)luaL_len(L, 3);
    const uint8_t* value = (uint8_t*)luaL_checkstring(L, 4);
    const size_t value_size = (size_t)luaL_len(L, 4);
    const int ttl = luaL_checkinteger(L, 5);
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
    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    const char* column_family = luaL_checkstring(L, 2);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 3);
    const size_t key_size = (size_t)luaL_len(L, 3);
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
    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    const char* column_family = luaL_checkstring(L, 2);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 3);
    const size_t key_size = (size_t)luaL_len(L, 3);

    tidesdb_err_t *ret = tidesdb_delete(db,
                                        column_family,
                                        key,
                                        key_size);
    LUA_RET_CODE()
}

static int compact_sstables(lua_State *L)
{
    lua_getfield(L, 1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    const char* column_family = luaL_checkstring(L, 2);
    const int max_threads = (uint32_t)luaL_checkinteger(L, 3);

    tidesdb_err_t *ret = tidesdb_compact_sstables(db,
                                        column_family,
                                        max_threads);
    LUA_RET_CODE()
}

static int list_column_families(lua_State *L)
{
    char* list = NULL;
    lua_getfield(L, -1, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_list_column_families(db, &list);
    LUA_RET_CODE_AND_VALUE(list, strlen(list));
}

static int txn_begin(lua_State *L)
{
    const char* column_family = luaL_checkstring(L, 2);
    tidesdb_txn_t *txn = NULL;

    lua_getfield(L, -2, "self_db");
    tidesdb_t *db = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_txn_begin(db, &txn, column_family);
    if(ret) {
        lua_pushinteger(L, ret->code);
        lua_pushstring(L, ret->message);
        tidesdb_err_free(ret);
        return 2;
    } else {
        lua_pushinteger(L, 0);
        lua_pushstring(L, "OK");

        lua_newtable(L);
        luaL_setfuncs(L, regs_tidesdb_txn_lua, 0);
        lua_pushlightuserdata(L, txn);
        lua_setfield(L, -2, "self_txn");
        return 3;
    }
}

static int txn_put(lua_State *L)
{
    lua_getfield(L, 1, "self_txn");
    tidesdb_txn_t *txn = lua_touserdata(L, -1);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 2);
    const size_t key_size = (size_t)luaL_len(L, 2);
    const uint8_t* value = (uint8_t*)luaL_checkstring(L, 3);
    const size_t value_size = (size_t)luaL_len(L, 3);
    const int ttl = luaL_checkinteger(L, 4);
    tidesdb_err_t *ret = tidesdb_txn_put(txn,
                                         key,
                                         key_size,
                                         value,
                                         value_size,
                                         ttl == -1 ? ttl : ttl + time(NULL));
    LUA_RET_CODE()
}

static int txn_delete(lua_State *L)
{
    lua_getfield(L, 1, "self_txn");
    tidesdb_txn_t *txn = lua_touserdata(L, -1);
    const uint8_t* key = (uint8_t*)luaL_checkstring(L, 2);
    const size_t key_size = (size_t)luaL_len(L, 2);
    tidesdb_err_t *ret = tidesdb_txn_delete(txn,
                                            key,
                                            key_size);
    LUA_RET_CODE()
}

static int txn_commit(lua_State *L)
{
    lua_getfield(L, -1, "self_txn");
    tidesdb_txn_t *txn = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_txn_commit(txn);
    LUA_RET_CODE()
}

static int txn_rollback(lua_State *L)
{
    lua_getfield(L, -1, "self_txn");
    tidesdb_txn_t *txn = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_txn_rollback(txn);
    LUA_RET_CODE()
}

static int txn_free(lua_State *L)
{
    lua_getfield(L, -1, "self_txn");
    tidesdb_txn_t *txn = lua_touserdata(L, -1);
    tidesdb_err_t *ret = tidesdb_txn_free(txn);
    LUA_RET_CODE()
}

LUALIB_API int luaopen_libtidesdb_lua(lua_State *L)
{
    luaL_newlib(L, regs_tidesdb_lib_lua);

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
