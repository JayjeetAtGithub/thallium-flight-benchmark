cmake_minimum_required(VERSION 3.2)

find_package (PkgConfig REQUIRED)
pkg_check_modules (MARGO REQUIRED IMPORTED_TARGET margo)
pkg_check_modules (ABT REQUIRED IMPORTED_TARGET argobots)
pkg_check_modules (ABTIO REQUIRED IMPORTED_TARGET abt-io)
pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)
pkg_check_modules (BAKECLIENT REQUIRED IMPORTED_TARGET bake-client)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(bs server.cc)
target_link_libraries(bs PkgConfig::BAKECLIENT)
