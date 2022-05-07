cmake_minimum_required(VERSION 3.2)
project(arrow-thallium)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)

add_executable(client client.cc)
target_link_libraries(client thallium)

add_executable(server server.cc)
target_link_libraries(server thallium)
