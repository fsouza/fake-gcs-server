cmake_minimum_required (VERSION 3.2)
set(CMAKE_CXX_STANDARD 11)
project (fake-gcs-server-cpp-examples)
include(FetchContent)

find_package(GTest REQUIRED)
find_package(absl CONFIG REQUIRED)

set(GOOGLE_CLOUD_CPP_ENABLE storage CACHE INTERNAL storage-api)
set(GOOGLE_CLOUD_CPP_ENABLE_MACOS_OPENSSL_CHECK OFF CACHE INTERNAL macos-openssl-check)
set(BUILD_TESTING OFF CACHE INTERNAL testing-off)
set(CRC32C_USE_GLOG OFF CACHE INTERNAL crc32c-glog-off)
set(CRC32C_BUILD_TESTS OFF CACHE INTERNAL crc32c-gtest-off)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE INTERNAL crc32-benchmarks-off)
set(CRC32C_INSTALL ON CACHE INTERNAL crc32-install-on)
FetchContent_Declare(
        crc32c
        URL     https://github.com/google/crc32c/archive/refs/tags/1.1.2.tar.gz
        URL_HASH SHA256=ac07840513072b7fcebda6e821068aa04889018f24e10e46181068fb214d7e56
)
FetchContent_MakeAvailable(crc32c)
add_library(Crc32c::crc32c ALIAS crc32c)
FetchContent_Declare(google-cloud-cpp
        URL                 https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v1.40.2.tar.gz
        URL_HASH            SHA256=394595d8ce0f17ad9f1a1e3efa1c54118a1246df617a7388da7111dfb299b05b)
FetchContent_MakeAvailable(google-cloud-cpp)

add_executable(cpp-example cpp-examples.cpp)
target_include_directories(cpp-example PUBLIC ${google-cloud-cpp_INCLUDE_DIRS})
target_link_libraries(cpp-example PUBLIC google-cloud-cpp::storage gtest)
gtest_add_tests(TARGET cpp-example)
