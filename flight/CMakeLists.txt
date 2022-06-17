cmake_minimum_required(VERSION 3.2)

find_package(Arrow REQUIRED)

add_executable(flight_client client.cc)
target_link_libraries(flight_client arrow arrow_dataset arrow_flight parquet)

add_executable(flight_server server.cc)
target_link_libraries(flight_server arrow arrow_dataset arrow_flight parquet)
