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
        URL     https://github.com/google/crc32c/archive/refs/tags/1.1.1.tar.gz
        URL_HASH SHA256=a6533f45b1670b5d59b38a514d82b09c6fb70cc1050467220216335e873074e8
)
FetchContent_MakeAvailable(crc32c)
add_library(Crc32c::crc32c ALIAS crc32c)
FetchContent_Declare(google-cloud-cpp
        URL                 https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v1.37.0.tar.gz
        URL_HASH            SHA256=a7269b21d5e95bebff7833ebb602bcd5bcc79e82a59449cc5d5b350ff2f50bbc)
FetchContent_MakeAvailable(google-cloud-cpp)

add_executable(cpp-example cpp-examples.cpp)
target_include_directories(cpp-example PUBLIC ${google-cloud-cpp_INCLUDE_DIRS})
target_link_libraries(cpp-example PUBLIC google-cloud-cpp::storage gtest)
gtest_add_tests(TARGET cpp-example)
