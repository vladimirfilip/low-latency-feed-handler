cmake_minimum_required(VERSION 3.16)
project(low_latency_feed_handler CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-fno-strict-aliasing)

add_executable(feed_handler ${CMAKE_SOURCE_DIR}/src/main.cpp)
target_include_directories(feed_handler PRIVATE ${CMAKE_SOURCE_DIR}/src)

# §5.1 replay tool: reads the .itch file and re-emits it as MoldUDP64-framed
# UDP, so feed_handler/ipc_producer_* can be exercised in socket mode
# (--udp=PORT) instead of (or as well as) mmap file mode.
add_executable(replay ${CMAKE_SOURCE_DIR}/replay/replay.cpp)
target_include_directories(replay PRIVATE ${CMAKE_SOURCE_DIR}/src)

# §6 IPC bus: producer/consumer are separate processes, compiled twice each —
# once against the SPSC shared-memory ring buffer, once against the AF_UNIX
# socket baseline — selected via a compile definition over a shared source
# file, so the latency comparison is transport vs. transport only.
find_library(LIBRT rt)

add_executable(ipc_producer_ring ${CMAKE_SOURCE_DIR}/ipc/producer_main.cpp)
target_include_directories(ipc_producer_ring PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/ipc)

add_executable(ipc_consumer_ring ${CMAKE_SOURCE_DIR}/ipc/consumer_main.cpp)
target_include_directories(ipc_consumer_ring PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/ipc)

if(LIBRT)
    target_link_libraries(ipc_producer_ring PRIVATE ${LIBRT})
    target_link_libraries(ipc_consumer_ring PRIVATE ${LIBRT})
endif()

add_executable(ipc_producer_unix ${CMAKE_SOURCE_DIR}/ipc/producer_main.cpp)
target_include_directories(ipc_producer_unix PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/ipc)
target_compile_definitions(ipc_producer_unix PRIVATE USE_UNIX_SOCKET)

add_executable(ipc_consumer_unix ${CMAKE_SOURCE_DIR}/ipc/consumer_main.cpp)
target_include_directories(ipc_consumer_unix PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/ipc)
target_compile_definitions(ipc_consumer_unix PRIVATE USE_UNIX_SOCKET)
