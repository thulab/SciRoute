add_test(
  NAME WebApiSmoke
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/web_api_smoke.sh" "$<TARGET_FILE:iotdb_fs_web>"
)

add_test(
  NAME PlatformConfigurationStatic
  COMMAND bash "${CMAKE_SOURCE_DIR}/tests/platform_config_check_test.sh" "${CMAKE_SOURCE_DIR}"
)
set_tests_properties(PlatformConfigurationStatic PROPERTIES
  LABELS "platform;configuration"
  TIMEOUT 20
)

set_tests_properties(WebApiSmoke PROPERTIES
  TIMEOUT 20
  LABELS integration
)

add_executable(iotdb_client_integration test_IoTDBClient.cpp)
target_link_libraries(iotdb_client_integration PRIVATE iotdb_fs_core)
add_test(NAME IoTDBClientHarnessHelp COMMAND iotdb_client_integration --help)
set_tests_properties(IoTDBClientHarnessHelp PROPERTIES LABELS integration TIMEOUT 15)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME OwnerReaderConfiguration
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_owner_reader_configuration.py"
      --bin "$<TARGET_FILE:iotdb_fs>")
  set_tests_properties(OwnerReaderConfiguration PROPERTIES LABELS "configuration" TIMEOUT 120)
  add_test(NAME QueryTaskConfiguration
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_query_task_configuration.py"
      --bin "$<TARGET_FILE:iotdb_fs>")
  set_tests_properties(QueryTaskConfiguration PROPERTIES LABELS "configuration" TIMEOUT 120)
  add_test(NAME ResultSenderConfiguration
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_result_sender_configuration.py"
      --bin "$<TARGET_FILE:iotdb_fs>")
  set_tests_properties(ResultSenderConfiguration PROPERTIES LABELS "configuration" TIMEOUT 120)
  add_custom_target(query_task_configuration
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_query_task_configuration.py"
      --bin "$<TARGET_FILE:iotdb_fs>"
    DEPENDS iotdb_fs
    USES_TERMINAL
  )
  add_executable(iotdb_fs_preflight
    ${CMAKE_SOURCE_DIR}/src/iotdb_fs_preflight.cpp
    ${CMAKE_SOURCE_DIR}/src/EnvironmentPreflight.cpp
  )
  target_include_directories(iotdb_fs_preflight PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    "${IOTDB_INCLUDE_DIR}"
  )
  target_link_libraries(iotdb_fs_preflight PRIVATE iotdb_fs_core)

  add_test(
    NAME EnvironmentPreflightHelp
    COMMAND iotdb_fs_preflight --help
  )
  set_tests_properties(EnvironmentPreflightHelp PROPERTIES
    LABELS integration
    TIMEOUT 15
  )
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME EnvironmentPreflightCli
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_preflight_cli.py"
      --bin "$<TARGET_FILE:iotdb_fs_preflight>"
      --fs-bin "$<TARGET_FILE:iotdb_fs>"
      --web-bin "$<TARGET_FILE:iotdb_fs_web>"
  )
  set_tests_properties(EnvironmentPreflightCli PROPERTIES LABELS integration TIMEOUT 120)
endif()

# Build the harness without requiring a live service in the default CTest suite.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(generic_fs_integration generic_fs_test.cpp)
  target_link_libraries(generic_fs_integration PRIVATE Threads::Threads)
  add_test(NAME GenericFsHarnessHelp COMMAND generic_fs_integration --help)
  set_tests_properties(GenericFsHarnessHelp PROPERTIES LABELS integration TIMEOUT 15)
  option(ENABLE_FUSE_INTEGRATION_TESTS "Register tests requiring an isolated IoTDB and FUSE" OFF)
  if(ENABLE_FUSE_INTEGRATION_TESTS)
    foreach(grib_enabled IN ITEMS true false)
      if(grib_enabled)
        set(generic_test_name GenericFsGribEnabled)
      else()
        set(generic_test_name GenericFsGribDisabled)
      endif()
      add_test(
        NAME ${generic_test_name}
        COMMAND generic_fs_integration
          --fs-bin "$<TARGET_FILE:iotdb_fs>"
          --web-bin "$<TARGET_FILE:iotdb_fs_web>"
          --pool-size 1
          --grib-enabled "${grib_enabled}"
      )
      set_tests_properties(${generic_test_name} PROPERTIES
        LABELS "integration;live;iotdb;fuse"
        RUN_SERIAL TRUE
        TIMEOUT 360
      )
    endforeach()
  endif()
endif()
