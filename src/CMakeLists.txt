cmake_minimum_required(VERSION 3.2)
project(arrow-thallium)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)

add_executable(client client.cc)
target_link_libraries(client thallium arrow arrow-dataset)

add_executable(server server.cc)
target_link_libraries(server thallium arrow arrow-dataset)
