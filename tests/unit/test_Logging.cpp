#include <gtest/gtest.h>
#include "iotdbfs/Logging.h"
#include "iotdbfs/Filesystem.h"

#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include "TestSupport.h"

namespace {

std::string read_file_text(const fs::path& path) {
  std::ifstream in(path);
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

fs::path make_log_test_dir(const std::string& name) {
  fs::path dir = fs::temp_directory_path() /
    ("iotdb-fs-log-" + name + "-" + std::to_string(now_ms()));
  std::error_code ec;
  fs::remove_all(dir, ec);
  fs::create_directories(dir, ec);
  return dir;
}

} // namespace

class LoggingTest : public ::testing::Test {
protected:
  LogLevel saved_;
  void SetUp() override {
    saved_ = get_logging_level();
  }
  void TearDown() override {
    set_logging_level(saved_);
  }
};

TEST_F(LoggingTest, FromString_Debug) {
  EXPECT_EQ(logging_level_from_string("DEBUG"), LogLevel::DEBUG);
}

TEST_F(LoggingTest, FromString_TraceAlias) {
  EXPECT_EQ(logging_level_from_string("TRACE"), LogLevel::DEBUG);
}

TEST_F(LoggingTest, FromString_Info) {
  EXPECT_EQ(logging_level_from_string("INFO"), LogLevel::INFO);
}

TEST_F(LoggingTest, FromString_Warn) {
  EXPECT_EQ(logging_level_from_string("WARN"), LogLevel::WARN);
}

TEST_F(LoggingTest, FromString_WarningAlias) {
  EXPECT_EQ(logging_level_from_string("WARNING"), LogLevel::WARN);
}

TEST_F(LoggingTest, FromString_Error) {
  EXPECT_EQ(logging_level_from_string("ERROR"), LogLevel::ERROR);
}

TEST_F(LoggingTest, FromString_ErrAlias) {
  EXPECT_EQ(logging_level_from_string("ERR"), LogLevel::ERROR);
}

TEST_F(LoggingTest, FromString_Off) {
  EXPECT_EQ(logging_level_from_string("OFF"), LogLevel::OFF);
}

TEST_F(LoggingTest, FromString_NoneAlias) {
  EXPECT_EQ(logging_level_from_string("NONE"), LogLevel::OFF);
}

TEST_F(LoggingTest, FromString_CaseInsensitive) {
  EXPECT_EQ(logging_level_from_string("debug"), LogLevel::DEBUG);
  EXPECT_EQ(logging_level_from_string("Info"), LogLevel::INFO);
  EXPECT_EQ(logging_level_from_string("WARN"), LogLevel::WARN);
  EXPECT_EQ(logging_level_from_string("error"), LogLevel::ERROR);
}

TEST_F(LoggingTest, FromString_UnknownDefaultsToInfo) {
  EXPECT_EQ(logging_level_from_string("unknown"), LogLevel::INFO);
}

TEST_F(LoggingTest, FromString_EmptyDefaultsToInfo) {
  EXPECT_EQ(logging_level_from_string(""), LogLevel::INFO);
}

TEST_F(LoggingTest, ToString_Debug) {
  EXPECT_EQ(logging_level_to_string(LogLevel::DEBUG), "DEBUG");
}

TEST_F(LoggingTest, ToString_Info) {
  EXPECT_EQ(logging_level_to_string(LogLevel::INFO), "INFO");
}

TEST_F(LoggingTest, ToString_Warn) {
  EXPECT_EQ(logging_level_to_string(LogLevel::WARN), "WARN");
}

TEST_F(LoggingTest, ToString_Error) {
  EXPECT_EQ(logging_level_to_string(LogLevel::ERROR), "ERROR");
}

TEST_F(LoggingTest, ToString_Off) {
  EXPECT_EQ(logging_level_to_string(LogLevel::OFF), "OFF");
}

TEST_F(LoggingTest, ToString_InvalidDefaultsToInfo) {
  EXPECT_EQ(logging_level_to_string(static_cast<LogLevel>(99)), "INFO");
}

TEST_F(LoggingTest, SetAndGet_Debug) {
  set_logging_level(LogLevel::DEBUG);
  EXPECT_EQ(get_logging_level(), LogLevel::DEBUG);
}

TEST_F(LoggingTest, SetAndGet_Error) {
  set_logging_level(LogLevel::ERROR);
  EXPECT_EQ(get_logging_level(), LogLevel::ERROR);
}

TEST_F(LoggingTest, SetAndGet_Off) {
  set_logging_level(LogLevel::OFF);
  EXPECT_EQ(get_logging_level(), LogLevel::OFF);
}

TEST_F(LoggingTest, RoundTrip_Debug) {
  auto lvl = logging_level_from_string("DEBUG");
  auto str = logging_level_to_string(lvl);
  auto lvl2 = logging_level_from_string(str);
  EXPECT_EQ(lvl, lvl2);
}

TEST_F(LoggingTest, RoundTrip_AllLevels) {
  std::vector<std::string> levels = {"DEBUG", "INFO", "WARN", "ERROR", "OFF"};
  for (const auto& l : levels) {
    auto lvl = logging_level_from_string(l);
    auto str = logging_level_to_string(lvl);
    auto lvl2 = logging_level_from_string(str);
    EXPECT_EQ(lvl, lvl2);
  }
}

TEST_F(LoggingTest, ThresholdWritesCombinedAndPerLevelLogs) {
  fs::path dir = make_log_test_dir("threshold");
  init_logging(dir.string());
  set_logging_level(LogLevel::WARN);

  log_common("DEBUG", "test", 1, "debug hidden");
  log_common("INFO", "test", 2, "info hidden");
  log_common("WARN", "test", 3, "warn visible");
  log_common("ERROR", "test", 4, "error visible");

  std::string combined = read_file_text(dir / "iotdb_fs.log");
  EXPECT_EQ(combined.find("debug hidden"), std::string::npos);
  EXPECT_EQ(combined.find("info hidden"), std::string::npos);
  EXPECT_NE(combined.find("warn visible"), std::string::npos);
  EXPECT_NE(combined.find("error visible"), std::string::npos);

  std::string warn_log = read_file_text(dir / "warn" / "warn.log");
  std::string error_log = read_file_text(dir / "error" / "error.log");
  EXPECT_NE(warn_log.find("warn visible"), std::string::npos);
  EXPECT_EQ(warn_log.find("error visible"), std::string::npos);
  EXPECT_NE(error_log.find("error visible"), std::string::npos);
  init_logging("");
  std::error_code error;
  fs::remove_all(dir, error);
}

class LoggingOutputTest : public LoggingTest {
protected:
  void SetUp() override {
    LoggingTest::SetUp();
    init_logging(directory_.file(""));
  }
  void TearDown() override {
    init_logging(""); // Close files before the temporary directory is removed.
    LoggingTest::TearDown();
  }
  test_support::TempDirectory directory_;
};

TEST_F(LoggingOutputTest, OffSuppressesAllActiveLogLevels) {
  set_logging_level(LogLevel::OFF);
  for (const auto* level : {"DEBUG", "INFO", "WARN", "ERROR"})
    log_common(level, "off_test", 12, "must not be written");
  EXPECT_FALSE(fs::exists(directory_.file("iotdb_fs.log")));
}

TEST_F(LoggingOutputTest, NormalizesAliasesAndNullLevelInWrittenRecords) {
  set_logging_level(LogLevel::DEBUG);
  log_common("trace", "alias_test", 17, "debug alias");
  log_common("warning", "alias_test", 18, "warn alias");
  log_common("err", "alias_test", 19, "error alias");
  log_common(nullptr, "alias_test", 20, "default info");
  for (const auto& item : std::vector<std::pair<std::string, std::string>>{
      {"debug", "DEBUG"}, {"warn", "WARN"}, {"error", "ERROR"}, {"info", "INFO"}}) {
    const auto contents = read_file_text(directory_.file(item.first + "/" + item.first + ".log"));
    EXPECT_NE(contents.find("[IOTDB-FS][" + item.second + "]"), std::string::npos);
    EXPECT_NE(contents.find("[alias_test:"), std::string::npos);
  }
}

TEST_F(LoggingOutputTest, ReinitializationRoutesNewMessagesWithoutTruncatingOldLogs) {
  set_logging_level(LogLevel::INFO);
  log_common("INFO", "routing", 1, "first record");
  init_logging(directory_.file(""));
  log_common("INFO", "routing", 2, "second record");
  test_support::TempDirectory other;
  init_logging(other.file(""));
  log_common("INFO", "routing", 3, "third record");
  const auto first = read_file_text(directory_.file("iotdb_fs.log"));
  EXPECT_NE(first.find("first record"), std::string::npos);
  EXPECT_NE(first.find("second record"), std::string::npos);
  EXPECT_EQ(first.find("third record"), std::string::npos);
  const auto second = read_file_text(other.file("iotdb_fs.log"));
  EXPECT_NE(second.find("third record"), std::string::npos);
  EXPECT_EQ(second.find("first record"), std::string::npos);
  init_logging(directory_.file(""));
  log_common("INFO", "routing", 4, "fourth record");
  EXPECT_NE(read_file_text(directory_.file("iotdb_fs.log")).find("first record"), std::string::npos);
}

TEST_F(LoggingOutputTest, InvalidOutputDirectoryDoesNotPreventLaterRecovery) {
  set_logging_level(LogLevel::INFO);
  const auto ordinary_file = directory_.write("ordinary", "preserved");
  init_logging(ordinary_file);
  EXPECT_NO_THROW(log_common("INFO", "recovery", 1, "cannot open output"));
  EXPECT_EQ(read_file_text(ordinary_file), "preserved");
  init_logging(directory_.file(""));
  log_common("INFO", "recovery", 2, "recovered record");
  EXPECT_NE(read_file_text(directory_.file("iotdb_fs.log")).find("recovered record"), std::string::npos);
}

TEST_F(LoggingOutputTest, ConcurrentWritersProduceCompleteUniqueRecords) {
  set_logging_level(LogLevel::INFO);
  std::vector<std::thread> writers;
  for (int writer = 0; writer < 4; ++writer) {
    writers.emplace_back([writer] {
      for (int record = 0; record < 20; ++record)
        log_common("INFO", "concurrent", record,
          "record=" + std::to_string(writer) + ":" + std::to_string(record) + ";");
    });
  }
  for (auto& writer : writers) writer.join();
  const auto combined = read_file_text(directory_.file("iotdb_fs.log"));
  EXPECT_EQ(combined, read_file_text(directory_.file("info/info.log")));
  EXPECT_EQ(std::count(combined.begin(), combined.end(), '\n'), 80);
  for (int writer = 0; writer < 4; ++writer) {
    for (int record = 0; record < 20; ++record) {
      const auto token = "record=" + std::to_string(writer) + ":" + std::to_string(record) + ";\n";
      const auto position = combined.find(token);
      ASSERT_NE(position, std::string::npos) << token;
      EXPECT_EQ(combined.find(token, position + token.size()), std::string::npos) << token;
    }
  }
}
