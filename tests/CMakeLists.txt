include(FetchContent)
FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.5.4
)
FetchContent_MakeAvailable(Catch2)

find_package(Threads REQUIRED)

add_executable(kvstore_tests
  storage_test.cpp
  protocol_test.cpp
  integration_test.cpp
  wal_test.cpp
  crash_recovery_test.cpp
  snapshot_test.cpp
  sharding_test.cpp
  replication_test.cpp
  failover_test.cpp
  rebalance_test.cpp
  kv_client.cpp
)
target_include_directories(kvstore_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore_tests PRIVATE
  kvstore_core
  Catch2::Catch2WithMain
  Threads::Threads
)

include(CTest)
include(Catch)
catch_discover_tests(kvstore_tests)
