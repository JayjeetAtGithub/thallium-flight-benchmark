cmake_minimum_required(VERSION 3.2)

project(arrow-data-transfer-protocol-benchmark)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3")

set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})

add_subdirectory(src)
add_subdirectory(flight)
