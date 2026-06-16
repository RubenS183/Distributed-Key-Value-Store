add_executable(kvstore_unit_tests unit/compile_smoke_test.cpp)
target_link_libraries(kvstore_unit_tests PRIVATE kvstore::core)
add_test(NAME unit.compile_smoke COMMAND kvstore_unit_tests)

add_executable(kvstore_integration_tests integration/server_smoke_test.cpp)
target_link_libraries(kvstore_integration_tests PRIVATE kvstore::core)
add_test(NAME integration.server_smoke COMMAND kvstore_integration_tests)

add_executable(kvstore_stress_tests stress/stress_smoke_test.cpp)
target_link_libraries(kvstore_stress_tests PRIVATE kvstore::core)
add_test(NAME stress.smoke COMMAND kvstore_stress_tests)
