# CURVE key handling comes from the xstore_curve static lib (top-level
# CMakeLists) -- the same objects the server links.
add_executable(xstore-keys main.cpp)
target_include_directories(xstore-keys PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore-keys PRIVATE xstore_curve)
