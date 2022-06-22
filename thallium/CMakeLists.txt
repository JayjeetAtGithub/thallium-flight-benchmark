cmake_minimum_required(VERSION 3.2)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)

add_executable(tc client.cc)
target_link_libraries(tc thallium arrow arrow_dataset)

add_executable(ts server.cc)
target_link_libraries(ts thallium arrow arrow_dataset)
