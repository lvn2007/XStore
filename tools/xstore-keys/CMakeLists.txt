# XStore_Curve.cpp must be compiled into this target too since it's not a
# library elsewhere yet (Task 4 -- see docs/superpowers/specs for context).
add_executable(xstore-keys main.cpp ${CMAKE_SOURCE_DIR}/src/XStore_Networking/XStore_Curve.cpp)
target_include_directories(xstore-keys PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore-keys PRIVATE "zmq")
