project("memcached-ebbrt" CXX)
cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g3")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4 -flto -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g3")

set(BAREMETAL_SOURCES 
  src/Memcached.cc 
  src/mcd.cpp)

set(BAREMETAL_INCLUDES 
  src/)

# Baremetal  ========================================================

if( ${CMAKE_SYSTEM_NAME} STREQUAL "EbbRT")

  message(STATUS "Building baremetal image...")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14 -Wall -Werror")
  include_directories(${BAREMETAL_INCLUDES})
  add_executable(memcached.elf ${BAREMETAL_SOURCES})
  add_custom_command(TARGET memcached.elf POST_BUILD 
    COMMAND objcopy -O elf32-i386 memcached.elf memcached.elf32 )

else()
  message(FATAL_ERROR "System name unsupported: ${CMAKE_SYSTEM_NAME}")
endif()
