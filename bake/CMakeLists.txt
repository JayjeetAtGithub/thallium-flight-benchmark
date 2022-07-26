cmake_minimum_required(VERSION 3.2)

find_package (PkgConfig REQUIRED)
pkg_check_modules (BAKECLIENT REQUIRED IMPORTED_TARGET bake-client)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(bs server.c)
target_link_libraries(ms PkgConfig::BAKECLIENT)
