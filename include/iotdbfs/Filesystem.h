#pragma once

// Compatibility header for std::filesystem (C++17)
// GCC < 8 requires <experimental/filesystem> and -lstdc++fs

#if __has_include(<filesystem>)
  #include <filesystem>
  namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #error "No filesystem header found. C++17 or experimental filesystem support required."
#endif
