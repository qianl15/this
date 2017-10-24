cmake_minimum_required(VERSION 3.7)
project(kernel_dev)

set(CMAKE_CXX_STANDARD 11)
find_package(Boost COMPONENTS thread program_options regex python REQUIRED)


include_directories(
        "."
        "${Boost_INCLUDE_DIRS}")

set(SOURCE_FILES main.cpp)
set (CMAKE_CXX_FLAGS "-DBOOST_ERROR_CODE_HEADER_ONLY")

add_executable(kernel_dev ${SOURCE_FILES})