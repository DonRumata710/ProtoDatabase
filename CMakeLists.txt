cmake_minimum_required(VERSION 3.14)

project(ProtoDatabase LANGUAGES CXX)

set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

macro(add_external_dependency localHints package)
    if(NOT EXISTS "${CMAKE_SOURCE_DIR}/3rdparty/${package}")
        list(APPEND CMAKE_PREFIX_PATH ${localHints}/${package})
        string(TOUPPER "${package}" uname)
        set(${uname}_ROOT_DIR "${localHints}${package}" CACHE PATH "path to ${package}")
        set(${package}_ROOT_DIR "${localHints}${package}" CACHE PATH "path to ${package}")
        message("Added ${package} to search paths")
    endif()
endmacro()

function(generate_protobuf_code export_macro sources)
    find_package(protobuf CONFIG)
    protobuf_generate(
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated
        PROTOS ${ARGN}
        OUT_VAR source_list
        EXPORT_MACRO ${export_macro}
    )
    set(${sources} ${source_list} PARENT_SCOPE)
    message("${ARGN}")
endfunction()


include("local.cmake")

if(NOT "${LOCAL_HINTS}" STREQUAL "")
    if(NOT DEFINED TARGET_ARCH)
        if(CMAKE_SIZEOF_VOID_P EQUAL 4)
            set(TARGET_ARCH "x32")
        else()
            set(TARGET_ARCH "x64")
        endif()
    endif()

    if(TARGET_ARCH STREQUAL "x32")
        set(_ARCH_SUFFIX "32")
    else()
        set(_ARCH_SUFFIX "64")
    endif()

    if(WIN32)
        set(_OS_SUFFIX "win")
    else()
        set(_OS_SUFFIX "linux")
    endif()

    if(MINGW)
        set(_COMPILE_FLAG "-mingw${_ARCH_SUFFIX}")
    endif()

    set(_LOCAL_HINTS "${LOCAL_HINTS}/${_OS_SUFFIX}${_ARCH_SUFFIX}${_COMPILE_FLAG}/")

    if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
        string(APPEND _LOCAL_HINTS "Debug/")
    else()
        string(APPEND _LOCAL_HINTS "Release/")
    endif()

    list(APPEND CMAKE_PREFIX_PATH ${_LOCAL_HINTS})

    file(GLOB packages RELATIVE ${_LOCAL_HINTS} ${_LOCAL_HINTS}/*)

    foreach(package ${packages})
        add_external_dependency(${_LOCAL_HINTS} ${package})
    endforeach()

    file(GLOB packages RELATIVE ${LOCAL_HINTS}/any ${LOCAL_HINTS}/any/*)

    foreach(package ${packages})
        add_external_dependency(${LOCAL_HINTS} ${package})
    endforeach()
    message("Prefix path: ${CMAKE_PREFIX_PATH}")
endif()

find_package(SQLiteCpp)
find_package(absl COMPONENTS strings REQUIRED)
find_package(protobuf COMPONENTS libprotobuf)
find_package(Catch2 COMPONENTS Catch2WithMain)

option(SHARED_LIBRARY true)

set(PUBLIC_HEADERS
    include/ProtoDatabase/Database.h
)

file(GLOB proto_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/proto/*)
generate_protobuf_code(EXPORT_ProtoDatabase source_list ${proto_files})

if (SHARED_LIBRARY)
    add_library(${PROJECT_NAME} SHARED
        ${PUBLIC_HEADERS}
        src/Database.cpp
        ${source_list}
        ${proto_files}
    )
else()
    add_library(${PROJECT_NAME} STATIC
        ${PUBLIC_HEADERS}
        src/Database.cpp
        ${source_list}
        ${proto_files}
    )
endif()

set_target_properties(${PROJECT_NAME} PROPERTIES
    PUBLIC_HEADER "${PUBLIC_HEADERS}"
    SOVERSION ${PROJECT_VERSION_MAJOR}
    VERSION ${PROJECT_VERSION}
)

set(LIBRARY_INSTALLATION_PATH ${PROJECT_NAME}/lib)
set(BINARY_INSTALLATION_PATH ${PROJECT_NAME}/bin)
set(INCLUDE_INSTALLATION_PATH ${PROJECT_NAME}/include/${PROJECT_NAME})

target_include_directories(${PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>
    $<INSTALL_INTERFACE:${INCLUDE_INSTALLATION_PATH}>
)

target_link_libraries(${PROJECT_NAME} PUBLIC SQLiteCpp absl::strings protobuf::libprotobuf)

if(SHARED_LIBRARY)
    if(WIN32)
        target_compile_definitions(${PROJECT_NAME} PRIVATE
            EXPORT_${PROJECT_NAME}=__declspec\(dllexport\)
        )
        target_compile_definitions(${PROJECT_NAME} INTERFACE
            EXPORT_${PROJECT_NAME}=__declspec\(dllimport\)
        )
    else()
        target_compile_definitions(${PROJECT_NAME} PUBLIC
            EXPORT_${PROJECT_NAME}=__attribute__\(\(visibility\(\"default\"\)\)\)
        )
    endif()
else()
    target_compile_definitions(${PROJECT_NAME} PUBLIC
        EXPORT_${PROJECT_NAME}=
    )
endif()

install(TARGETS ${PROJECT_NAME}
    EXPORT ${PROJECT_NAME}Targets
    LIBRARY DESTINATION ${LIBRARY_INSTALLATION_PATH} COMPONENT Runtime
    ARCHIVE DESTINATION ${LIBRARY_INSTALLATION_PATH} COMPONENT Development
    RUNTIME DESTINATION ${BINARY_INSTALLATION_PATH} COMPONENT Runtime
    PUBLIC_HEADER DESTINATION ${INCLUDE_INSTALLATION_PATH} COMPONENT Development
    BUNDLE DESTINATION ${BINARY_INSTALLATION_PATH} COMPONENT Runtime
)

install(
    DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated/
    DESTINATION ${INCLUDE_INSTALLATION_PATH}
)

file(GLOB install_proto_files ${CMAKE_CURRENT_SOURCE_DIR}/res/*)
install(FILES ${install_proto_files}
    DESTINATION ${LIBRARY_INSTALLATION_PATH}/proto
)

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION 0.1
    COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
    "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION ${LIBRARY_INSTALLATION_PATH}/cmake/${PROJECT_NAME}
)

install(EXPORT ${PROJECT_NAME}Targets DESTINATION ${LIBRARY_INSTALLATION_PATH}/cmake/${PROJECT_NAME} DESTINATION ${LIBRARY_INSTALLATION_PATH}/cmake/${PROJECT_NAME})

install(FILES "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake" "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    DESTINATION ${LIBRARY_INSTALLATION_PATH}/cmake/${PROJECT_NAME}
)

file(GLOB SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)

foreach(FILE ${SOURCES})
    get_filename_component(TEST ${FILE} NAME_WE)
    set(TEST "${PROJECT_NAME}-test-${TEST}")

    add_executable(${TEST} ${FILE})

    target_link_libraries(${TEST} PRIVATE
        ${TEST_DEPENDENCIES}
    )

    if(NOT isApp)
        target_link_libraries(${TEST} PRIVATE
            ${PROJECT_NAME}
            Catch2::Catch2WithMain
        )
    endif()

    target_include_directories(${TEST} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}/_generated
        ${ARG_INCLUDES}
    )

    set_target_properties(${TEST} PROPERTIES
        CXX_STANDARD 20
    )

    message("Add test: ${TEST}")
    add_test(NAME ${TEST} COMMAND ${TEST})
endforeach()
