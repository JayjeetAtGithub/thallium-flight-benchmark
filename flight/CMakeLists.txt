cmake_minimum_required(VERSION 3.2)

find_package(Arrow REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(fc client.cc)
target_link_libraries(fc arrow arrow_dataset arrow_flight parquet)

add_executable(fs server.cc)
target_link_libraries(fs arrow arrow_dataset arrow_flight parquet)
