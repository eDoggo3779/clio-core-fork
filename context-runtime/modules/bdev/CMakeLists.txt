cmake_minimum_required(VERSION 3.10)

# Collect optional NIXL dependency
set(BDEV_OPTIONAL_LIBS "")
if(CLIO_CORE_ENABLE_NIXL)
  list(APPEND BDEV_OPTIONAL_LIBS ctp::nixl)
endif()

# Collect optional CUDA dependency for kHbm/kPinned bdev types
set(BDEV_GPU_LIBS "")
if(CLIO_CORE_ENABLE_CUDA)
  list(APPEND BDEV_GPU_LIBS ctp::cuda_cxx)
elseif(CLIO_CORE_ENABLE_ROCM)
  list(APPEND BDEV_GPU_LIBS ctp::rocm_cxx)
endif()

# Collect optional S3 backend dependency (kS3 bdev type, prototype).
# Hand-rolled S3 client: libcurl for HTTP + OpenSSL for SigV4 signing.
set(BDEV_S3_LIBS "")
set(BDEV_RUNTIME_EXTRA_SOURCES "")
if(CLIO_CORE_ENABLE_S3)
  find_package(CURL REQUIRED)
  find_package(OpenSSL REQUIRED)
  list(APPEND BDEV_S3_LIBS CURL::libcurl OpenSSL::Crypto)
  list(APPEND BDEV_RUNTIME_EXTRA_SOURCES src/s3_client.cc)
  message(STATUS "bdev: S3 backend enabled (libcurl + OpenSSL SigV4)")
endif()

# Hand-rolled GCS client (kGcs bdev type, prototype): libcurl for the GCS JSON
# API + OpenSSL for the OAuth2 service-account JWT (RSA-SHA256) auth path.
if(CLIO_CORE_ENABLE_GCS)
  find_package(CURL REQUIRED)
  find_package(OpenSSL REQUIRED)
  list(APPEND BDEV_S3_LIBS CURL::libcurl OpenSSL::Crypto)
  list(APPEND BDEV_RUNTIME_EXTRA_SOURCES src/gcs_client.cc)
  message(STATUS "bdev: GCS backend enabled (libcurl + OpenSSL OAuth2/JWT)")
endif()

# Hand-rolled Azure Blob client (kAzure / kAzurePage bdev types, prototype):
# libcurl for HTTP + OpenSSL for the Shared Key signer (HMAC-SHA256, base64).
if(CLIO_CORE_ENABLE_AZURE)
  find_package(CURL REQUIRED)
  find_package(OpenSSL REQUIRED)
  list(APPEND BDEV_S3_LIBS CURL::libcurl OpenSSL::Crypto)
  list(APPEND BDEV_RUNTIME_EXTRA_SOURCES src/azure_blob_client.cc)
  message(STATUS "bdev: Azure Blob backend enabled (libcurl + OpenSSL Shared Key)")
endif()

# Hand-rolled Alibaba Cloud OSS client (kOss bdev type, prototype): libcurl for
# HTTP + OpenSSL for the native OSS V1 signer (HMAC-SHA1, base64) plus a SigV4
# path (HMAC-SHA256) for OSS's S3-compatible endpoint / local emulators.
if(CLIO_CORE_ENABLE_OSS)
  find_package(CURL REQUIRED)
  find_package(OpenSSL REQUIRED)
  list(APPEND BDEV_S3_LIBS CURL::libcurl OpenSSL::Crypto)
  list(APPEND BDEV_RUNTIME_EXTRA_SOURCES src/oss_client.cc)
  message(STATUS "bdev: OSS backend enabled (libcurl + OpenSSL OSS V1 HMAC-SHA1)")
endif()

# Client library — ctp::aio provides async I/O headers via its INTERFACE includes.
# LIB_NAME pins the library to `clio_bdev_client` (the new brand-aligned
# name); ALIAS keeps `chimaera_bdev_client` working as a CMake target for
# any in-tree or out-of-tree consumer that still references the old name.
add_clio_module_client(
  LIB_NAME clio_bdev
  SOURCES
    src/bdev_client.cc
  LINK_LIBRARIES
    clio_admin_client
    ctp::aio
    ${BDEV_OPTIONAL_LIBS}
)
add_library(chimaera_bdev_client ALIAS clio_bdev_client)

# Runtime library (see client block above for the LIB_NAME + ALIAS rationale).
add_clio_module_runtime(
  LIB_NAME clio_bdev
  SOURCES
    src/bdev_runtime.cc
    src/autogen/bdev_lib_exec.cc
    ${BDEV_RUNTIME_EXTRA_SOURCES}
  LINK_LIBRARIES
    clio_admin_runtime
    ctp::aio
    ${BDEV_OPTIONAL_LIBS}
    ${BDEV_GPU_LIBS}
    ${BDEV_S3_LIBS}
)
add_library(chimaera_bdev_runtime ALIAS clio_bdev_runtime)

# Compile the kS3 backend (and its conditional Runtime members) into the
# runtime target only when S3 is enabled. PRIVATE keeps the macro/ABI scoped
# to the runtime library's translation units (bdev_runtime.cc + autogen).
if(CLIO_CORE_ENABLE_S3 AND TARGET clio_bdev_runtime)
  target_compile_definitions(clio_bdev_runtime PRIVATE CLIO_BDEV_S3_ENABLED=1)
endif()
if(CLIO_CORE_ENABLE_GCS AND TARGET clio_bdev_runtime)
  target_compile_definitions(clio_bdev_runtime PRIVATE CLIO_BDEV_GCS_ENABLED=1)
endif()
if(CLIO_CORE_ENABLE_AZURE AND TARGET clio_bdev_runtime)
  target_compile_definitions(clio_bdev_runtime PRIVATE CLIO_BDEV_AZURE_ENABLED=1)
endif()
if(CLIO_CORE_ENABLE_OSS AND TARGET clio_bdev_runtime)
  target_compile_definitions(clio_bdev_runtime PRIVATE CLIO_BDEV_OSS_ENABLED=1)
endif()

# SYCL HBM bdev support: a separate task. Adding -fsycl to
# clio_bdev_runtime conflicts with the runtime's precompiled
# headers (clang-offload-bundler rejects .pch). When that's resolved
# (likely needs PCH disabled for SYCL builds), enable here:
#
#   if(CLIO_CORE_ENABLE_SYCL AND TARGET clio_bdev_runtime)
#     set_target_properties(clio_bdev_runtime PROPERTIES
#       DISABLE_PRECOMPILE_HEADERS ON)
#     wrp_core_apply_sycl_flags(clio_bdev_runtime)
#   endif()
#
# Until then, the kHbm bdev path under SYCL is compiled out by the
# `#if CTP_ENABLE_SYCL && !(CUDA||ROCM)` guard in src/bdev_runtime.cc.

# Unit tests
if(CLIO_CORE_ENABLE_TESTS)
  add_subdirectory(test)
endif()
