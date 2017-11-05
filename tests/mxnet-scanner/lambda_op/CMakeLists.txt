# To build your custom op, you can either use our CMake convenience methods
# or do it the slightly harder way in normal Make (see the Makefile).

cmake_minimum_required(VERSION 3.7 FATAL_ERROR)

# Uncomment the line below and change the Scanner path to the repo you cloned:
set(SCANNER_PATH /opt/scanner)
if(NOT SCANNER_PATH)
  message(FATAL_ERROR "You need to update the SCANNER_PATH in mxnetlambda_op/CMakeLists.txt first.")
endif()
set(CMAKE_CXX_STANDARD 11)
include(${SCANNER_PATH}/cmake/Util/Op.cmake)

find_package(Boost COMPONENTS thread program_options regex python REQUIRED)
find_package(OpenSSL REQUIRED)
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

message(${OPENSSL_LIBRARIES})

if(UNIX)
  set(PLATFORM_LINK_FLAGS "-pthread")
elseif(APPLE)
  set(PLATFORM_LINK_FLAGS "")
endif()

# build_op will create a shared library called lib${LIB_NAME}.so that builds
# from all of the CPP_SRCS. You can also optionally specify a PROTO_SRC that
# points to a Protobuf file and will generate the C++ and Python bindings.
build_op(
  LIB_NAME mxnetlambda_op
  CPP_SRCS mxnetlambda_op.cpp
  PROTO_SRC mxnetlambda.proto)

# The library specified in build_op is a normal CMake target, so you can use all
# the normal CMake functions with it.
find_package(OpenCV REQUIRED COMPONENTS core imgproc)
include_directories(SYSTEM ${OpenCV_INCLUDE_DIRS} "."
        "${Boost_INCLUDE_DIRS}"
        "${OPENSSL_INCLUDE_DIR}")

target_link_libraries(mxnetlambda_op PUBLIC "${OpenCV_LIBRARIES}" 
	"${OPENSSL_LIBRARIES}" "${PLATFORM_LINK_FLAGS}" "${Boost_THREAD_LIBRARY}" 
	"${Boost_SYSTEM_LIBRARY}")
