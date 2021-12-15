cmake_minimum_required(VERSION 3.20)
project(xStore)

set(CMAKE_CXX_STANDARD 17)

include_directories(.)
include_directories(src)

add_executable(XStore src/main.cpp src/XStore/XStore.cpp include/XStore/XStore.h src/dataStruct/dataStruct.cpp include/dataStruct/dataStruct.h)

if(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 9)
    target_link_libraries(XStore PUBLIC "stdc++fs")
endif()