cmake_minimum_required(VERSION 3.2)

find_package(mercury REQUIRED)
find_package(bake REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(bs server.c)
target_link_libraries(bs bake)
