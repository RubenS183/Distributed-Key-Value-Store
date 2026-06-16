cmake_minimum_required(VERSION 3.22)

project(kv_store
  VERSION 0.1.0
  DESCRIPTION "Distributed key-value database skeleton"
  LANGUAGES CXX
)

option(KVSTORE_BUILD_TESTS "Build tests" ON)
option(KVSTORE_BUILD_BENCHMARKS "Build benchmarks" ON)
option(KVSTORE_ENABLE_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(kvstore_core)
add_library(kvstore::core ALIAS kvstore_core)

target_sources(kvstore_core
  PRIVATE
    src/cache/lru_cache.cpp
    src/concurrency/thread_pool.cpp
    src/metrics/metrics_registry.cpp
    src/network/tcp_server.cpp
    src/persistence/persistence_manager.cpp
    src/protocol/protocol_parser.cpp
    src/replication/replication_manager.cpp
    src/server/server.cpp
    src/storage/kv_engine.cpp
    src/utils/logger.cpp
)

target_include_directories(kvstore_core
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(kvstore_core PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(kvstore_core PRIVATE /W4)
  if(KVSTORE_ENABLE_WARNINGS_AS_ERRORS)
    target_compile_options(kvstore_core PRIVATE /WX)
  endif()
else()
  target_compile_options(kvstore_core PRIVATE -Wall -Wextra -Wpedantic)
  if(KVSTORE_ENABLE_WARNINGS_AS_ERRORS)
    target_compile_options(kvstore_core PRIVATE -Werror)
  endif()
endif()

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore::core)

include(CTest)
if(KVSTORE_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(KVSTORE_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
