cmake_minimum_required(VERSION 3.16)
project(ecewo_postgres VERSION 0.1.0 LANGUAGES C)

add_library(ecewo_postgres
  src/ecewo-postgres.c
)

add_library(ecewo::postgres ALIAS ecewo_postgres)

target_include_directories(ecewo_postgres PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

if (PROJECT_IS_TOP_LEVEL AND NOT TARGET ecewo::ecewo)
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

target_include_directories(ecewo_postgres PRIVATE
  ${PostgreSQL_INCLUDE_DIRS}
)

if (PROJECT_IS_TOP_LEVEL AND TARGET ecewo::ecewo)
  target_link_libraries(ecewo_postgres
    PRIVATE
      ecewo::ecewo
      ecewo::postgres
  )
else()
  target_link_libraries(ecewo_postgres PUBLIC
    ecewo::ecewo
    ${PostgreSQL_LIBRARIES}
  )
endif()
