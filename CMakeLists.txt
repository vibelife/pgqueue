cmake_minimum_required(VERSION 3.21)
project(pgqueue)

set(CMAKE_CXX_STANDARD 20)
set(Boost_NO_WARN_NEW_VERSIONS 1)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines -march=haswell")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")

add_executable(${PROJECT_NAME}
        main.cpp
        src/MPMCQueue.hpp
        src/PGQueryProcessor.hpp src/PGQueryParams.hpp src/PGQueryStructures.hpp src/PGEventHandler.hpp src/PGConnection.hpp src/PGConnectionPool.hpp)

find_package(PkgConfig REQUIRED)
find_package(Boost REQUIRED COMPONENTS context system regex)
link_libraries(${Boost_LIBRARIES})

find_package(Threads)
link_libraries(${Threads})

pkg_check_modules(URING liburing)
find_library(URING uring)
link_libraries(${URING})

find_package(PostgreSQL)
include_directories(/usr/include/postgresql/)


target_link_libraries(${PROJECT_NAME} ${FMT_LIBRARIES} Boost::context PostgreSQL::PostgreSQL ${CMAKE_THREAD_LIBS_INIT} ${URING})
