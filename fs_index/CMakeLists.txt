cmake_minimum_required(VERSION 3.16)
project(FSIndex LANGUAGES C CXX)

# This migration retains the validated Linux profile and native dependencies.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "FS-index's validated native profile requires Linux (or WSL2).")
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(HDF5 1.14.4 EXACT CONFIG REQUIRED COMPONENTS C shared)
find_package(SQLite3 REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
find_path(ECCODES_INCLUDE_DIR eccodes.h PATH_SUFFIXES x86_64-linux-gnu REQUIRED)
find_library(ECCODES_LIBRARY eccodes REQUIRED)
if(NOT TARGET hdf5-shared OR NOT HDF5_ENABLE_THREADSAFE)
  message(FATAL_ERROR "Require product-matched thread-safe HDF5 1.14.4")
endif()

add_library(fs_index_native SHARED src/native.cpp)
target_include_directories(fs_index_native PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(fs_index_native PRIVATE ${ECCODES_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
target_compile_options(fs_index_native PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(fs_index_native PRIVATE hdf5-shared SQLite::SQLite3 ${ECCODES_LIBRARY} OpenSSL::Crypto)

option(FS_INDEX_BUILD_TESTS "Run FS-index's native GRIB/HDF5 functional fixtures" OFF)
if(FS_INDEX_BUILD_TESTS)
  # Fault injection is absent from production builds.
  target_compile_definitions(fs_index_native PRIVATE FS_INDEX_TEST_FAULTS=1)
  find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter)
  execute_process(COMMAND "${Python3_EXECUTABLE}" -c "import h5py, numpy"
    RESULT_VARIABLE FS_INDEX_PYTHON_STATUS)
  if(NOT FS_INDEX_PYTHON_STATUS EQUAL 0)
    message(FATAL_ERROR "FS-index tests require h5py and NumPy in Python3_EXECUTABLE")
  endif()
  enable_testing()
  add_test(NAME fs_index_native_functional
    COMMAND "${CMAKE_COMMAND}" -E env
      "FS_INDEX_NATIVE_LIBRARY=$<TARGET_FILE:fs_index_native>"
      "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/.."
      "PYTHONDONTWRITEBYTECODE=1"
      "${Python3_EXECUTABLE}" -m unittest fs_index.tests.test_native -v)
  set_tests_properties(fs_index_native_functional PROPERTIES
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/.."
    LABELS "fs-index;unit" TIMEOUT 120)
  add_test(NAME fs_index_maintenance
    COMMAND "${CMAKE_COMMAND}" -E env
      "FS_INDEX_NATIVE_LIBRARY=$<TARGET_FILE:fs_index_native>"
      "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/.."
      "PYTHONDONTWRITEBYTECODE=1"
      "${Python3_EXECUTABLE}" -m unittest fs_index.tests.test_maintenance -v)
  set_tests_properties(fs_index_maintenance PROPERTIES
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/.."
    LABELS "fs-index;unit" TIMEOUT 180)
endif()
