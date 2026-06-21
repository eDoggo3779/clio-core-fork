# CMakeLists.txt for bdev module unit tests
cmake_minimum_required(VERSION 3.10)

# Bdev ChiMod test executable
set(BDEV_TEST_TARGET chimaera_bdev_chimod_tests)
set(BDEV_TEST_SOURCES
  test_bdev_chimod.cc
)

# GPU bdev tests (test_bdev_gpucache, test_bdev_gpu_kernels) removed
# along with the GPU runtime concept.

# Create bdev test executable
add_executable(${BDEV_TEST_TARGET} ${BDEV_TEST_SOURCES})

# Include directories for bdev tests
target_include_directories(${BDEV_TEST_TARGET} PRIVATE
  ${CLIO_RUN_ROOT}/include
  ${CLIO_RUN_ROOT}/test  # For simple_test.h
  ${CLIO_RUN_ROOT}/modules/admin/include  # For admin tasks
  ${CLIO_RUN_ROOT}/modules/bdev/include   # For bdev tasks and client
)

# Link against required libraries for bdev tests
target_link_libraries(${BDEV_TEST_TARGET}
  clio_admin_runtime          # Admin module runtime
  clio_admin_client           # Admin module client
  clio_bdev_runtime           # Bdev module runtime
  clio_bdev_client            # Bdev module client
  ctp::cxx                       # ClioCtp library
  ${CMAKE_THREAD_LIBS_INIT}       # Threading support
)

# Set C++ standard for bdev tests
set_target_properties(${BDEV_TEST_TARGET} PROPERTIES
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
)

# Copy bdev test executable to bin directory
set_target_properties(${BDEV_TEST_TARGET} PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# GPU bdev test build rules removed.

# Enable CTest integration if testing is enabled
if(CLIO_CORE_ENABLE_TESTS)
  # Comprehensive bdev test (runs all tests; explicit TCP/IPC auto-skip
  # because default IPC mode is SHM and the skip guards filter them out)
  add_test(
    NAME cr_all_bdev_tests
    COMMAND ${BDEV_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_all_bdev_tests PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )

  # Dedicated explicit backend tests — each sets CHI_IPC_MODE so the
  # corresponding TEST_CASE runs while the other two auto-skip.
  add_test(
    NAME cr_bdev_explicit_backend_shm
    COMMAND ${BDEV_TEST_TARGET} "bdev_file_explicit_backend_shm"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_explicit_backend_shm PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CHI_IPC_MODE=SHM;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )

  add_test(
    NAME cr_bdev_explicit_backend_tcp
    COMMAND ${BDEV_TEST_TARGET} "bdev_file_explicit_backend_tcp"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_explicit_backend_tcp PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CHI_IPC_MODE=TCP;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )

  add_test(
    NAME cr_bdev_explicit_backend_ipc
    COMMAND ${BDEV_TEST_TARGET} "bdev_file_explicit_backend_ipc"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_explicit_backend_ipc PROPERTIES
    TIMEOUT 180
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CHI_IPC_MODE=IPC;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )

  # Set test properties
  set_tests_properties(
    cr_all_bdev_tests
    cr_bdev_explicit_backend_shm
    cr_bdev_explicit_backend_tcp
    cr_bdev_explicit_backend_ipc
    PROPERTIES
      TIMEOUT 300
      LABELS "msan_skip"
      ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )

  # Force-net stress test: routes all bdev tasks through the network path
  # even on a single node, exercising SendIn/RecvIn/SendOut/RecvOut on loopback.
  add_test(
    NAME cr_bdev_force_net
    COMMAND ${BDEV_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_force_net PROPERTIES
    TIMEOUT 300
    LABELS "msan_skip"
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500;CLIO_FORCE_NET=1"
  )
endif()

# Custom targets for bdev tests
add_custom_target(test_bdev
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev ChiMod tests"
)

add_custom_target(test_bdev_create
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev container creation tests"
)

add_custom_target(test_bdev_allocate
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev block allocation tests"
)

add_custom_target(test_bdev_io
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev I/O operation tests"
)

add_custom_target(test_bdev_async
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev asynchronous I/O tests"
)

add_custom_target(test_bdev_performance
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev performance metrics tests"
)

add_custom_target(test_bdev_error
  COMMAND ${BDEV_TEST_TARGET}
  DEPENDS ${BDEV_TEST_TARGET}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  COMMENT "Running Chimaera bdev error handling tests"
)

# S3 backend roundtrip test (prototype) — built only when S3 is enabled.
# Self-skips at runtime unless S3_ENDPOINT is set, so it is safe in CI.
if(CLIO_CORE_ENABLE_S3)
  set(BDEV_S3_TEST_TARGET chimaera_bdev_s3_tests)
  add_executable(${BDEV_S3_TEST_TARGET} test_bdev_s3.cc)
  target_include_directories(${BDEV_S3_TEST_TARGET} PRIVATE
    ${CLIO_RUN_ROOT}/include
    ${CLIO_RUN_ROOT}/test
    ${CLIO_RUN_ROOT}/modules/admin/include
    ${CLIO_RUN_ROOT}/modules/bdev/include
  )
  target_link_libraries(${BDEV_S3_TEST_TARGET}
    clio_admin_runtime
    clio_admin_client
    clio_bdev_runtime
    clio_bdev_client
    ctp::cxx
    ${CMAKE_THREAD_LIBS_INIT}
  )
  set_target_properties(${BDEV_S3_TEST_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  add_test(
    NAME cr_bdev_s3_tests
    COMMAND ${BDEV_S3_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_s3_tests PROPERTIES
    TIMEOUT 300
    LABELS "msan_skip"
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )
  install(TARGETS ${BDEV_S3_TEST_TARGET} RUNTIME DESTINATION bin)
endif()

# GCS backend roundtrip test (prototype) — built only when GCS is enabled.
# Self-skips at runtime unless GCS_ENDPOINT is set, so it is safe in CI.
if(CLIO_CORE_ENABLE_GCS)
  set(BDEV_GCS_TEST_TARGET chimaera_bdev_gcs_tests)
  add_executable(${BDEV_GCS_TEST_TARGET} test_bdev_gcs.cc)
  target_include_directories(${BDEV_GCS_TEST_TARGET} PRIVATE
    ${CLIO_RUN_ROOT}/include
    ${CLIO_RUN_ROOT}/test
    ${CLIO_RUN_ROOT}/modules/admin/include
    ${CLIO_RUN_ROOT}/modules/bdev/include
  )
  target_link_libraries(${BDEV_GCS_TEST_TARGET}
    clio_admin_runtime
    clio_admin_client
    clio_bdev_runtime
    clio_bdev_client
    ctp::cxx
    ${CMAKE_THREAD_LIBS_INIT}
  )
  set_target_properties(${BDEV_GCS_TEST_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  add_test(
    NAME cr_bdev_gcs_tests
    COMMAND ${BDEV_GCS_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_gcs_tests PROPERTIES
    TIMEOUT 300
    LABELS "msan_skip"
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )
  install(TARGETS ${BDEV_GCS_TEST_TARGET} RUNTIME DESTINATION bin)

  # Offline GCS unit tests (JWT signer, JSON parsing, ADC precedence, retry
  # policy). No network and no runtime init, so this ALWAYS runs (unlike the
  # endpoint-gated trace test above) — it is the primary correctness proof for
  # the auth/retry code real GCS exercises.
  set(BDEV_GCS_UNIT_TARGET chimaera_bdev_gcs_unit_tests)
  add_executable(${BDEV_GCS_UNIT_TARGET} test_gcs_unit.cc)
  target_include_directories(${BDEV_GCS_UNIT_TARGET} PRIVATE
    ${CLIO_RUN_ROOT}/include
    ${CLIO_RUN_ROOT}/test
    ${CLIO_RUN_ROOT}/modules/admin/include
    ${CLIO_RUN_ROOT}/modules/bdev/include
  )
  target_link_libraries(${BDEV_GCS_UNIT_TARGET}
    clio_admin_runtime
    clio_admin_client
    clio_bdev_runtime
    clio_bdev_client
    ctp::cxx
    CURL::libcurl
    OpenSSL::Crypto
    ${CMAKE_THREAD_LIBS_INIT}
  )
  set_target_properties(${BDEV_GCS_UNIT_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  add_test(
    NAME cr_bdev_gcs_unit_tests
    COMMAND ${BDEV_GCS_UNIT_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_gcs_unit_tests PROPERTIES
    TIMEOUT 120
    LABELS "msan_skip"
  )
  install(TARGETS ${BDEV_GCS_UNIT_TARGET} RUNTIME DESTINATION bin)
endif()

# Azure Blob backend roundtrip test (prototype) — built only when Azure is
# enabled. Self-skips at runtime unless AZURE_BLOB_ENDPOINT is set (CI-safe).
if(CLIO_CORE_ENABLE_AZURE)
  set(BDEV_AZURE_TEST_TARGET chimaera_bdev_azure_tests)
  add_executable(${BDEV_AZURE_TEST_TARGET} test_bdev_azure.cc)
  target_include_directories(${BDEV_AZURE_TEST_TARGET} PRIVATE
    ${CLIO_RUN_ROOT}/include
    ${CLIO_RUN_ROOT}/test
    ${CLIO_RUN_ROOT}/modules/admin/include
    ${CLIO_RUN_ROOT}/modules/bdev/include
  )
  target_link_libraries(${BDEV_AZURE_TEST_TARGET}
    clio_admin_runtime
    clio_admin_client
    clio_bdev_runtime
    clio_bdev_client
    ctp::cxx
    ${CMAKE_THREAD_LIBS_INIT}
  )
  set_target_properties(${BDEV_AZURE_TEST_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  add_test(
    NAME cr_bdev_azure_tests
    COMMAND ${BDEV_AZURE_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_azure_tests PROPERTIES
    TIMEOUT 300
    LABELS "msan_skip"
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10500"
  )
  install(TARGETS ${BDEV_AZURE_TEST_TARGET} RUNTIME DESTINATION bin)
endif()

# Alibaba Cloud OSS backend test (prototype) — built only when OSS is enabled.
# The native OSS V1 signer test-vector case always runs (no network); the
# data-plane roundtrip/sparse cases self-skip unless OSS_ENDPOINT is set.
if(CLIO_CORE_ENABLE_OSS)
  set(BDEV_OSS_TEST_TARGET chimaera_bdev_oss_tests)
  add_executable(${BDEV_OSS_TEST_TARGET} test_bdev_oss.cc)
  target_include_directories(${BDEV_OSS_TEST_TARGET} PRIVATE
    ${CLIO_RUN_ROOT}/include
    ${CLIO_RUN_ROOT}/test
    ${CLIO_RUN_ROOT}/modules/admin/include
    ${CLIO_RUN_ROOT}/modules/bdev/include
  )
  target_link_libraries(${BDEV_OSS_TEST_TARGET}
    clio_admin_runtime
    clio_admin_client
    clio_bdev_runtime
    clio_bdev_client
    ctp::cxx
    ${CMAKE_THREAD_LIBS_INIT}
  )
  set_target_properties(${BDEV_OSS_TEST_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  add_test(
    NAME cr_bdev_oss_tests
    COMMAND ${BDEV_OSS_TEST_TARGET}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
  )
  set_tests_properties(cr_bdev_oss_tests PROPERTIES
    TIMEOUT 300
    LABELS "msan_skip"
    ENVIRONMENT "CHI_REPO_PATH=${CMAKE_BINARY_DIR}/bin;LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH};CHIMAERA_TEST_MODE=1;CLIO_BIND_ADDR=127.0.0.1;CLIO_PORT=10502"
  )
  install(TARGETS ${BDEV_OSS_TEST_TARGET} RUNTIME DESTINATION bin)
endif()

# Install test executable
install(TARGETS ${BDEV_TEST_TARGET}
  RUNTIME DESTINATION bin
)

message(STATUS "Bdev module tests configured:")
message(STATUS "  Test target: ${BDEV_TEST_TARGET}")
message(STATUS "  Test sources: ${BDEV_TEST_SOURCES}")
