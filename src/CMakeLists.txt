find_package(Threads REQUIRED)

add_library(kvstore_core STATIC
  store.cpp
  protocol.cpp
  server.cpp
  wal.cpp
  snapshot.cpp
  sharding.cpp
  sharded_store.cpp
  replication.cpp
  failover.cpp
  rebalance.cpp
)
target_include_directories(kvstore_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore_core PUBLIC Threads::Threads)

add_executable(kvstore_server main.cpp)
target_link_libraries(kvstore_server PRIVATE kvstore_core)
