cmake_minimum_required(VERSION 3.22)
project(os3)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include_directories(.)
include_directories(Resources/Barrier)

add_executable(os3
        MapReduceFramework.cpp
        MapReduceClient.h
        MapReduceFramework.h
        Resources/Barrier/Barrier.cpp
        Resources/Barrier/Barrier.h)
