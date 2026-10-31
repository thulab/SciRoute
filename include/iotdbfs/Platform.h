#pragma once
#include <cstddef>
#include <cstdint>
#include <filesystem>
#if !defined(_WIN32)
#include <sys/types.h>
#else
#include <cstdint>
using ssize_t = std::intptr_t;
#endif
namespace iotdbfs::platform {
int create_staged_file(const char* dir);
int close_fd(int fd);
ssize_t read_at(int fd, void* buffer, size_t size, uint64_t offset);
ssize_t write_at(int fd, const void* buffer, size_t size, uint64_t offset);
int resize_fd(int fd, uint64_t size);
int sync_fd(int fd);
std::filesystem::path executable_path(const char* argv0);
int set_environment(const char* name, const char* value, bool overwrite = true);
bool xattr_write_supported();
}
