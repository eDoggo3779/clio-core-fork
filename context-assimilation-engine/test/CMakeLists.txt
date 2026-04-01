#------------------------------------------------------------------------------
# CAE Tests CMakeLists.txt
#------------------------------------------------------------------------------
# Top-level test configuration for Content Assimilation Engine
#
# This file adds the unit test subdirectory when BUILD_TESTING is enabled.
# Unit tests validate core CAE functionality including:
# - File assimilation (binary, HDF5)
# - Transfer operations (full file, range-based)
# - Error handling and validation
# - Integration with external services (Globus)
#------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)

# Only build tests if BUILD_TESTING is enabled
if(BUILD_TESTING)
  message(STATUS "Building CAE unit tests")
  add_subdirectory(unit)
else()
  message(STATUS "Skipping CAE tests (BUILD_TESTING is OFF)")
endif()
