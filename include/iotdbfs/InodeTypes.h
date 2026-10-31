#pragma once

#include <string>

// Flat representation of a filesystem entry persisted inside IoTDB.
struct Inode {
  long long row_time = 0;
  std::string path;
  std::string parent;
  std::string name;
  bool is_dir = false;
  long long size = 0;
  int mode = 0644;
  int uid = 0;
  int gid = 0;
  int nlink = 1;
  long long ctime = 0;
  long long mtime = 0;
  long long atime = 0;
  // Stable format identity selected from file content during ingestion. Empty
  // means that no registered adapter recognized the payload.
  std::string format_id;
  // Canonical typed file-metadata document. The core treats the document as a
  // format-neutral value; only adapters and generic metadata services project
  // its fields.
  std::string metadata;
};
