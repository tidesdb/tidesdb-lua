cmake_minimum_required(VERSION 3.25)
project(tidesdb_lua C)

set(CMAKE_C_STANDARD 23)
set(PROJECT_VERSION 0.1.0)
set(TIDEDB_INC "../tidesdb/src/")
add_compile_options(-Wextra -Wall)

find_package(Lua 5.3 REQUIRED)

find_library(LIBRARY_TIDEDB NAMES libtidesdb.so PATHS  "../tidesdb/build" NO_DEFAULT_PATH)

add_library(tidesdb_lua SHARED tidesdb-lua.c)
target_include_directories(tidesdb_lua PRIVATE ${TIDEDB_INC})
target_include_directories(tidesdb_lua PRIVATE ${LUA_INCLUDE_DIR})
target_link_libraries(tidesdb_lua ${LIBRARY_TIDEDB})
target_link_libraries(tidesdb_lua ${LUA_LIBRARIES})
