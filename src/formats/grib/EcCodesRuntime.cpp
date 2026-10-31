#include "iotdbfs/formats/grib/EcCodesRuntime.h"

#include "iotdbfs/Filesystem.h"
#include "iotdbfs/Platform.h"

#include <eccodes.h>
#include <cstdlib>
#include <string>
#include <system_error>
#include <vector>

namespace {

bool has_boot_def(const fs::path& definitions) {
  std::error_code ec;
  return fs::is_regular_file(definitions / "boot.def", ec);
}

bool has_sample_files(const fs::path& samples) {
  std::error_code ec;
  return fs::is_directory(samples, ec);
}

void add_candidate(std::vector<fs::path>& candidates, const fs::path& p) {
  if (!p.empty())
    candidates.push_back(p);
}

} // namespace

namespace iotdbfs {

bool configure_eccodes_runtime(const char* argv0, std::string* message) {
  const char* existing = std::getenv("ECCODES_DEFINITION_PATH");
  if (existing && *existing && has_boot_def(fs::path(existing))) {
    if (message)
      *message = std::string("Using ECCODES_DEFINITION_PATH=") + existing;
    return true;
  }

  std::vector<fs::path> candidates;
  fs::path exe = iotdbfs::platform::executable_path(argv0);
  if (!exe.empty()) {
    fs::path bin_dir = exe.parent_path();
    fs::path root = bin_dir.parent_path();
    add_candidate(candidates, root / "share/eccodes/definitions");
    add_candidate(candidates, root / "resources/eccodes/definitions");
    add_candidate(candidates, bin_dir / "../share/eccodes/definitions");
  }

  std::error_code ec;
  fs::path cwd = fs::current_path(ec);
  if (!cwd.empty()) {
    add_candidate(candidates, cwd / "share/eccodes/definitions");
    add_candidate(candidates, cwd / "resources/eccodes/definitions");
    add_candidate(candidates,
      cwd / "third_party/eccodes-static/share/eccodes/definitions");
  }

  for (const auto& candidate : candidates) {
    fs::path defs = fs::weakly_canonical(candidate, ec);
    if (ec)
      defs = candidate;
    if (!has_boot_def(defs))
      continue;

    fs::path samples = defs.parent_path() / "samples";
    iotdbfs::platform::set_environment("ECCODES_DEFINITION_PATH", defs.string().c_str());
    codes_context_set_definitions_path(codes_context_get_default(),
      defs.string().c_str());

    if (has_sample_files(samples)) {
      iotdbfs::platform::set_environment("ECCODES_SAMPLES_PATH", samples.string().c_str());
      codes_context_set_samples_path(codes_context_get_default(),
        samples.string().c_str());
    }

    if (message)
      *message = std::string("Configured ecCodes definitions path: ") +
        defs.string();
    return true;
  }

  if (message) {
    *message = "ecCodes definitions path not found; package share/eccodes or "
      "set ECCODES_DEFINITION_PATH to a directory containing boot.def";
  }
  return false;
}

} // namespace iotdbfs
