cmake_minimum_required(VERSION 3.25)
project(tidesdb_lua C)

set(CMAKE_C_STANDARD 23)
set(PROJECT_VERSION 0.1.0)
add_compile_options(-Wextra -Wall)

# set CMP0156 policy
cmake_policy(SET CMP0156 NEW)

find_package(Lua 5.3 REQUIRED)
find_package(tidesdb REQUIRED)

add_library(tidesdb_lua SHARED tidesdb-lua.c)
target_include_directories(tidesdb_lua PRIVATE ${LUA_INCLUDE_DIR} ${tidesdb_INCLUDE_DIRS})
target_link_libraries(tidesdb_lua ${tidesdb_LIBRARIES} ${LUA_LIBRARIES})