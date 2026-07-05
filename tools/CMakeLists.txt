add_executable(kvstore_bench_restore bench_restore.cpp)
target_link_libraries(kvstore_bench_restore PRIVATE kvstore_core)

add_executable(kvstore_bench_concurrency bench_concurrency.cpp)
target_link_libraries(kvstore_bench_concurrency PRIVATE kvstore_core)

add_executable(kvstore_bench_rebalance bench_rebalance.cpp)
target_link_libraries(kvstore_bench_rebalance PRIVATE kvstore_core)

# Throughput/latency bench talks to a real server over TCP, so it reuses the
# test-only wire client (tests/kv_client.*) rather than duplicating framing.
add_executable(kvstore_bench_throughput
  bench_throughput.cpp
  ${CMAKE_SOURCE_DIR}/tests/kv_client.cpp)
target_include_directories(kvstore_bench_throughput PRIVATE ${CMAKE_SOURCE_DIR}/tests)
target_link_libraries(kvstore_bench_throughput PRIVATE kvstore_core)
