# To build your custom op, you can either use our CMake convenience methods
# or do it the slightly harder way in normal Make (see the Makefile).

cmake_minimum_required(VERSION 3.2.0 FATAL_ERROR)

# Uncomment the line below and change the Scanner path to the repo you cloned:
set(SCANNER_PATH /opt/scanner)
if(NOT SCANNER_PATH)
  message(FATAL_ERROR "You need to update the SCANNER_PATH in mxnetlambda_op/CMakeLists.txt first.")
endif()
include(${SCANNER_PATH}/cmake/Util/Op.cmake)

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
include_directories(SYSTEM ${OpenCV_INCLUDE_DIRS})
target_link_libraries(mxnetlambda_op PUBLIC "${OpenCV_LIBRARIES}")
