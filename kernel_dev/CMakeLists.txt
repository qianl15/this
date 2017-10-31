cmake_minimum_required(VERSION 3.7)
project(kernel_dev)

set(CMAKE_CXX_STANDARD 11)
#add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY -DOPENSSL_ROOT_DIR=/usr/local/Cellar/openssl/1.0.2l -DOPENSSL_LIBRARIES=/usr/local/Cellar/openssl/1.0.2l/lib -DOPENSSL_INCLUDE_DIR=/usr/local/Cellar/openssl/1.0.2l/include)

#add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY -DOPENSSL_ROOT_DIR=/usr/local/Cellar/openssl/1.0.2l)

find_package(Boost COMPONENTS thread program_options regex python REQUIRED)
find_package(openssl REQUIRED)

include(CheckLibraryExists)
CHECK_LIBRARY_EXISTS(${OPENSSL_SSL_LIBRARY} "TLSv1_2_client_method" "" OPENSSL_TLSV12)
CHECK_LIBRARY_EXISTS(${OPENSSL_SSL_LIBRARY} "TLSv1_1_client_method" "" OPENSSL_TLSV11)
CHECK_LIBRARY_EXISTS(${OPENSSL_SSL_LIBRARY} "TLSv1_client_method" "" OPENSSL_TLSV10)

# Add a define based on the highest TLS version found. Fatal if no TLS client.
if(OPENSSL_TLSV12)
    add_definitions(-DSSL_TXT_TLSV1_2)
elseif(OPENSSL_TLSV11)
    add_definitions(-DSSL_TXT_TLSV1_1)
elseif(OPENSSL_TLSV10)
    add_definitions(-DSSL_TXT_TLSV1)
else()
    message(FATAL "Cannot find any TLS client methods")
endif()

include_directories(
        "."
        "${Boost_INCLUDE_DIRS}"
        "${OPENSSL_INCLUDE_DIR}")

set(SOURCE_FILES main.cpp)
set(CMAKE_CXX_FLAGS "-DBOOST_ERROR_CODE_HEADER_ONLY")
#set(CMAKE_EXE_LINKER_FLAGS "-L/usr/local/opt/openssl/lib")
#set(CMAKE_CXX_FLAGS "-DBOOST_ERROR_CODE_HEADER_ONLY -I/usr/local/opt/openssl/include")
#set(CMAKE_EXE_LINKER_FLAGS "-L/usr/local/Cellar/openssl/1.0.2l/lib")

add_executable(kernel_dev ${SOURCE_FILES})
message(${OPENSSL_LIBRARIES})
target_link_libraries(kernel_dev ${OPENSSL_LIBRARIES})
