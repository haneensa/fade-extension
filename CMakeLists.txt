cmake_minimum_required(VERSION 3.5)

# Set extension name
set(TARGET_NAME fade)

# Find OpenSSL
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Include directories
include_directories(src/include)

# List of source files
set(EXTENSION_SOURCES 
    src/lineage/lineage_init.cpp
    src/fade_extension.cpp
    src/lineage/lineage_reader.cpp
    src/lineage/lineage_meta.cpp
    src/lineage/logical_lineage_operator.cpp
    src/lineage/physical_lineage_operator.cpp
    src/lineage/physical_caching_operator.cpp
    src/fade/fade_node.cpp
    src/fade/fade_reader.cpp
    src/fade/fade_sparse.cpp
)

# Build static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})

# Link OpenSSL
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Install targets
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
