#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include "iotdbfs/Filesystem.h"

using namespace std;
// namespace fs handled by Filesystem.h

size_t getFileSize(const string& path) {
  ifstream file(path, ios::binary | ios::ate);
  if (!file.is_open()) {
    cerr << "❌ Failed to open: " << path << endl;
    return 0;
  }
  return file.tellg();
}

void verifyFile(const string& path, size_t expected) {
  size_t size = getFileSize(path);
  cout << "🔹 " << path << " => " << size << " bytes";
  if (expected != 0) {
    if (size == expected) cout << " ✅ (match expected)";
    else cout << " ❌ (expected " << expected << ")";
  }
  cout << endl;
}

int main() {
  cout << "=== GRIB2 FUSE Filesystem Test ===" << endl;

  vector<pair<string, size_t>> tests = {
      {"show.grib2?function=read&type=binary&level=500", 66606},
      {"show.grib2?function=read&level=500&dtype=f32", 4152960},
      {"show.grib2?function=read&level=500&dtype=f64", 8305920},
      {"show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64", 4001920},
      {"show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64", 26568},
      {"show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32", 4152960},
      {"show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64", 256000}
  };

  fs::current_path("/mnt/dfs");
  cout << "Working directory: " << fs::current_path() << endl;

  for (const auto& [path, expected] : tests) {
    verifyFile(path, expected);
  }

  cout << "=== Test Finished ===" << endl;
  return 0;
}
