cmake_minimum_required(VERSION 3.20)
project(xStore)

set(CMAKE_CXX_STANDARD 17)

include_directories(.)
include_directories(src)

add_executable(xStore src/main.cpp)

if(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 9)
    target_link_libraries(xStore PUBLIC "stdc++fs")
endif()