cmake_minimum_required(VERSION 3.2)

find_package(mercury REQUIRED)
find_package(thallium REQUIRED)
find_package(yokan REQUIRED)
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED)
find_package (PkgConfig REQUIRED)
pkg_check_modules (MARGO REQUIRED IMPORTED_TARGET margo)
pkg_check_modules (ABT REQUIRED IMPORTED_TARGET argobots)
pkg_check_modules (ABTIO REQUIRED IMPORTED_TARGET abt-io)
pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(rtc client.cc)
target_link_libraries(rtc thallium)
add_executable(rtc1 client_1.cc)
target_link_libraries(rtc1 thallium)

add_executable(rts server.cc)
target_link_libraries(rts thallium)
add_executable(rts1 server_1.cc)
target_link_libraries(rts1 thallium)
