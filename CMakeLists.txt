cmake_minimum_required(VERSION 3.15)
project(W7Practical C)

set(CMAKE_C_STANDARD 11)

include_directories(src)

add_executable(W7Practical
        src/myfs.c
        src/myfs.h
        src/test.c
        src/unqlite.c
        src/unqlite.h)
