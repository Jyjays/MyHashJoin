# 最低 CMake 版本要求
cmake_minimum_required(VERSION 3.10)

# 项目名称
project(MyHashJoin)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找 Google Test 包
find_package(GTest REQUIRED)
if (NOT GTest_FOUND)
    message(FATAL_ERROR "Google Test not found. Please install it (e.g., sudo apt-get install libgtest-dev).")
endif()

# 包含 Google Test 的头文件路径
include_directories(${GTEST_INCLUDE_DIRS})

# 添加 include 目录到头文件搜索路径
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# 添加源文件
set(SOURCES
    src/hashjoin.cpp
)

# 创建库（方便复用）
add_library(hashjoin STATIC ${SOURCES})
target_include_directories(hashjoin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 创建测试可执行文件
add_executable(hashjoin_test test/hashjoinTest.cpp)

# 链接库
target_link_libraries(hashjoin_test
    hashjoin
    ${GTEST_LIBRARIES}
    ${GTEST_MAIN_LIBRARIES}
    pthread 
)

# 启用测试
enable_testing()
add_test(NAME HashJoinTest COMMAND hashjoin_test)

# 可选：设置输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)