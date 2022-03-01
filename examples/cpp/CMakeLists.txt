cmake_minimum_required (VERSION 3.2)
set(CMAKE_CXX_STANDARD 20)
project (fake-gcs-server-cpp-examples)
include(FetchContent)

set(GOOGLE_CLOUD_CPP_ENABLE storage CACHE INTERNAL storage-api)
set(GOOGLE_CLOUD_CPP_ENABLE_MACOS_OPENSSL_CHECK OFF CACHE INTERNAL macos-openssl-check)
set(BUILD_TESTING OFF CACHE INTERNAL testing-off)
#TODO replace b03448411402c83082c279b246548d578dee05c7 with the next release which contains it
FetchContent_Declare(google-cloud-cpp
        GIT_REPOSITORY      git@github.com:googleapis/google-cloud-cpp.git
        GIT_TAG             b03448411402c83082c279b246548d578dee05c7)
FetchContent_MakeAvailable(google-cloud-cpp)

add_executable(upload-example upload.cpp)
target_link_libraries(upload-example google-cloud-cpp::storage)

add_executable(list-bucket-example list_bucket.cpp)
target_link_libraries(list-bucket-example google-cloud-cpp::storage)
