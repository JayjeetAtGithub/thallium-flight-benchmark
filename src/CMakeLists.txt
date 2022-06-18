cmake_minimum_required(VERSION 3.2)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)

add_executable(thallium_client client.cc)
target_link_libraries(thallium_client thallium arrow arrow_dataset)

add_executable(thallium_server server.cc)
target_link_libraries(thallium_server thallium arrow arrow_dataset)
