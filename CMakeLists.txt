cmake_minimum_required(VERSION 3.21)
project(pgqueue)

set(CMAKE_CXX_STANDARD 20)
set(Boost_NO_WARN_NEW_VERSIONS 1)
set(BOOST_ROOT /usr/include/boost_1_81_0)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")

add_executable(${PROJECT_NAME}
    main.cpp
    src/MPMCQueue.hpp
    src/PGQueryProcessor.hpp
    src/PGQueryParams.hpp
    src/PGQueryStructures.hpp
    src/PGConnection.hpp
    src/PGConnectionPool.hpp
    src/common/TimeUtils.hpp
    src/PGQueryProcessingState.hpp src/Consts.hpp)

find_package(Boost REQUIRED COMPONENTS context system)
link_libraries(${Boost_LIBRARIES})

find_package(Threads)
link_libraries(${Threads})

find_package(PostgreSQL)
include_directories(/usr/include/postgresql/current)


target_link_libraries(${PROJECT_NAME} Boost::context PostgreSQL::PostgreSQL ${CMAKE_THREAD_LIBS_INIT})
