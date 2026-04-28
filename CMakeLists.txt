cmake_minimum_required(VERSION 3.16)
project(ecewo_postgres VERSION 0.2.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Choose the type of build." FORCE)
endif()

if(PROJECT_IS_TOP_LEVEL)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

option(ECEWO_PG_BUILD_SHARED "Build ecewo-postgres as a shared library" OFF)

if(PROJECT_IS_TOP_LEVEL AND NOT TARGET ecewo::ecewo)
  include(FetchContent)

  FetchContent_Declare(
    ecewo
    GIT_REPOSITORY https://github.com/savashn/ecewo.git
    GIT_TAG v4
    GIT_SHALLOW TRUE
  )

  FetchContent_MakeAvailable(ecewo)
endif()

find_package(PostgreSQL REQUIRED)

if(ECEWO_PG_BUILD_SHARED)
  add_library(ecewo_postgres SHARED ecewo-postgres.c)
  message(STATUS "Building ecewo-postgres as SHARED library")
else()
  add_library(ecewo_postgres STATIC ecewo-postgres.c)
  message(STATUS "Building ecewo-postgres as STATIC library")
endif()

include(GenerateExportHeader)
generate_export_header(ecewo_postgres
  BASE_NAME ECEWO_PG
  EXPORT_FILE_NAME "${CMAKE_CURRENT_BINARY_DIR}/include/ecewo-pg-export.h"
)

add_library(ecewo::postgres ALIAS ecewo_postgres)

target_compile_definitions(ecewo_postgres PRIVATE ECEWO_PG_BUILDING)

if(ECEWO_PG_BUILD_SHARED)
  target_compile_definitions(ecewo_postgres PUBLIC ECEWO_PG_SHARED)
else()
  target_compile_definitions(ecewo_postgres PUBLIC ECEWO_PG_STATIC_DEFINE)
endif()

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(ecewo_postgres PRIVATE
    -Wunused-function
    -Wunused-variable
    -Werror=unused-function
    -Werror=unused-variable
  )
endif()

target_include_directories(ecewo_postgres
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(ecewo_postgres
  PUBLIC
    ecewo::ecewo
  PRIVATE
    ${PostgreSQL_LIBRARIES}
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(ecewo_postgres PRIVATE ECEWO_DEBUG=1)
endif()

set_target_properties(ecewo_postgres PROPERTIES
  OUTPUT_NAME ecewo_postgres
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  EXPORT_NAME postgres
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
  POSITION_INDEPENDENT_CODE ON
)
