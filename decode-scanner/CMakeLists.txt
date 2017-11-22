cmake_minimum_required(VERSION 3.2.0 FATAL_ERROR)

project(decoder_scanner)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(OpenCV REQUIRED COMPONENTS core imgproc highgui xfeatures2d imgcodecs)
find_package(Storehouse REQUIRED CONFIG
  PATHS "/opt/scanner/thirdparty/build/bin/storehouse")
find_package(FFmpeg REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(SaneProtobuf REQUIRED)

set(SCANNER_INCLUDE "/opt/scanner/build" "/opt/scanner")
set(SCANNER_LIBRARY "/opt/scanner/build/libscanner.so")
set(PLATFORM_LINK_FLAGS "-pthread -ldl -lrt")

include_directories(SYSTEM ${OpenCV_INCLUDE_DIRS} ${SCANNER_INCLUDE} ${STOREHOUSE_INCLUDE_DIRS} ${FFMPEG_INCLUDE_DIR} ${OPENSSL_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIRS} ".")

add_executable(DecoderAutomataCmd decoder_automata_cmd.cpp)
target_link_libraries(DecoderAutomataCmd ${SCANNER_LIBRARY} ${STOREHOUSE_LIBRARIES} ${FFMPEG_LIBRARIES} "-L/opt/ffmpeg-3.3.1/lib" ${OPENSSL_LIBRARIES}  ${OpenCV_LIBRARIES} ${PROTOBUF_LIBRARY} ${PLATFORM_LINK_FLAGS})