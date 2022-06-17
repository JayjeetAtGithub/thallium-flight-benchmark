cmake_minimum_required(VERSION 3.2)
project(arrow-flight)

find_package(Arrow REQUIRED)

add_executable(client client.cc)
target_link_libraries(client arrow arrow_dataset arrow_flight parquet)

add_executable(server server.cc)
target_link_libraries(server arrow arrow_dataset arrow_flight parquet)
