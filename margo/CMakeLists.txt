cmake_minimum_required(VERSION 3.2)

find_package (PkgConfig REQUIRED)
pkg_check_modules (MARGO REQUIRED IMPORTED_TARGET margo)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(mc client.c)
target_link_libraries(mc PkgConfig::MARGO)

add_executable(ms server.c)
target_link_libraries(ms PkgConfig::MARGO)
