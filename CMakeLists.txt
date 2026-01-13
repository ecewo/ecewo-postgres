cmake_minimum_required(VERSION 3.16)
project(ecewo_postgres VERSION 0.1.0 LANGUAGES C)

add_library(ecewo_postgres
  src/ecewo-postgres.c
)

add_library(ecewo::postgres ALIAS ecewo_postgres)

target_include_directories(ecewo_postgres PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

find_package(PostgreSQL REQUIRED)

target_include_directories(ecewo_postgres PRIVATE
  ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(ecewo_postgres PUBLIC
  ecewo::ecewo
  ${PostgreSQL_LIBRARIES}
)
