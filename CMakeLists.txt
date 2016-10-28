project("memcached-ebbrt" C CXX)
cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g3")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4 -flto -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14 -Wall -Werror")

set(BAREMETAL_SOURCES 
  src/Memcached.cc 
  src/mcd.cpp)

set(BAREMETAL_INCLUDES 
  src/)

set(HOSTED_SOURCES
  Memcached.cc)

# Baremetal  ========================================================

if( ${CMAKE_SYSTEM_NAME} STREQUAL "EbbRT")

  message(STATUS "### BUILDING NATIVE ###")
  
  include_directories(${BAREMETAL_INCLUDES})
  add_executable(memcached.elf ${BAREMETAL_SOURCES})
  add_custom_command(TARGET memcached.elf POST_BUILD 
    COMMAND objcopy -O elf32-i386 memcached.elf memcached.elf32 )
  
elseif( ${CMAKE_SYSTEM_NAME} STREQUAL "Linux" )
  
  message(STATUS "### BUILDING HOSTED ###")

  set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
  find_package(EbbRT REQUIRED)
  find_package(Boost 1.53.0 REQUIRED COMPONENTS
    filesystem system coroutine context )
  find_package(Capnp REQUIRED)
  find_package(TBB REQUIRED)
  find_package(Threads REQUIRED)
  find_package(EbbRTCmdLine REQUIRED)
  
  
  include_directories(${EBBRT_INCLUDE_DIRS})
  add_executable(Memcached Memcached.cc)
  target_link_libraries(Memcached ${EBBRT-CMDLINE_LIBRARIES}
    ${EBBRT_LIBRARIES} ${CAPNP_LIBRARIES_LITE} ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES} ${TBB_LIBRARIES}
  )
  
else()
  message(FATAL_ERROR "System name unsupported: ${CMAKE_SYSTEM_NAME}")
endif()
