cmake_minimum_required(VERSION 3.20)
project(kvstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra)

option(KVSTORE_BUILD_TESTS "Build unit tests" ON)
option(KVSTORE_ENABLE_TSAN "Build with ThreadSanitizer instrumentation" OFF)

if(KVSTORE_ENABLE_TSAN)
  add_compile_options(-fsanitize=thread -g -O1)
  add_link_options(-fsanitize=thread)
endif()

add_subdirectory(src)
add_subdirectory(tools)

if(KVSTORE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
