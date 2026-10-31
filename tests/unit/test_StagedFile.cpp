#include "iotdbfs/StagedFile.h"

#include <gtest/gtest.h>

#include <array>

TEST(StagedFile, RandomWritesPreserveUntouchedBytesAndSparseHoles) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abcdef", 6, 0), 0);
  ASSERT_EQ(file.write("XY", 2, 2), 0);
  ASSERT_EQ(file.write("z", 1, 9), 0);
  char bytes[10]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 10);
  EXPECT_EQ(std::string(bytes, 10), std::string("abXYef\0\0\0z", 10));
  EXPECT_EQ(file.sync(), 0);
}

TEST(StagedFile, TruncateAndExtendDoNotResurrectDiscardedBytes) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abcdef", 6, 0), 0);
  ASSERT_EQ(file.resize(2), 0);
  ASSERT_EQ(file.resize(6), 0);
  char bytes[6]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 6);
  EXPECT_EQ(std::string(bytes, 6), std::string("ab\0\0\0\0", 6));
  EXPECT_EQ(file.read(bytes, sizeof(bytes), 6), 0);
}

TEST(StagedFile, LoadsInBoundedChunksAndRejectsShortReads) {
  iotdbfs::StagedFile file;
  constexpr uint64_t size = 3 * 1024 * 1024 + 17;
  size_t calls = 0;
  ASSERT_EQ(file.load(size, [&](uint64_t off, uint64_t len) {
    EXPECT_LE(len, 1024u * 1024u);
    ++calls;
    return std::optional<std::vector<uint8_t>>(
      std::vector<uint8_t>(len, static_cast<uint8_t>(off / (1024 * 1024))));
  }), 0);
  EXPECT_EQ(calls, 4u);
  EXPECT_EQ(file.size(), size);
  uint8_t byte = 0;
  ASSERT_EQ(file.read(&byte, 1, size - 1), 1);
  EXPECT_EQ(byte, 3);
  iotdbfs::StagedFile short_file;
  EXPECT_EQ(short_file.load(8, [](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>(7));
  }), -EIO);
}

TEST(StagedFile, RejectsOffsetOverflow) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  EXPECT_EQ(file.write("a", 1, UINT64_MAX), -EFBIG);
  EXPECT_EQ(file.resize(UINT64_MAX), -EFBIG);
}

TEST(StagedFile, UncreatedFileRejectsIoAndSync) {
  iotdbfs::StagedFile file;
  char byte = 'x';
  EXPECT_EQ(file.size(), 0u);
  EXPECT_EQ(file.read(&byte, 1, 0), -EBADF);
  EXPECT_EQ(file.write(&byte, 1, 0), -EBADF);
  EXPECT_EQ(file.resize(1), -EBADF);
  EXPECT_EQ(file.sync(), -EBADF);
}

TEST(StagedFile, RepeatedCreatePreservesExistingData) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("hello", 5, 0), 0);
  ASSERT_EQ(file.create(), 0);
  char bytes[5]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 5);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), "hello");
  EXPECT_EQ(file.size(), 5u);
}

TEST(StagedFile, PartialReadStopsAtEofAndPreservesRemainingBuffer) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abcde", 5, 0), 0);
  std::array<char, 8> bytes;
  bytes.fill('!');
  ASSERT_EQ(file.read(bytes.data(), bytes.size(), 3), 2);
  EXPECT_EQ(std::string(bytes.data(), bytes.size()), "de!!!!!!");
  EXPECT_EQ(file.read(bytes.data(), bytes.size(), 5), 0);
  EXPECT_EQ(file.read(bytes.data(), bytes.size(), UINT64_MAX), 0);
  EXPECT_EQ(std::string(bytes.data(), bytes.size()), "de!!!!!!");
}

TEST(StagedFile, ZeroLengthIoDoesNotExtendFileOrChangeData) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abc", 3, 0), 0);
  EXPECT_EQ(file.write(nullptr, 0, 1024), 0);
  EXPECT_EQ(file.read(nullptr, 0, 1), 0);
  EXPECT_EQ(file.size(), 3u);
  char bytes[3]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 3);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), "abc");
}

TEST(StagedFile, OverwritePreservesSizeAndEmbeddedNulBytes) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  const std::string original("a\0bc\0d", 6);
  const std::string replacement("\0Z", 2);
  ASSERT_EQ(file.write(original.data(), original.size(), 0), 0);
  ASSERT_EQ(file.write(replacement.data(), replacement.size(), 2), 0);
  EXPECT_EQ(file.size(), original.size());
  char bytes[6]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 6);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), std::string("a\0\0Z\0d", 6));
}

TEST(StagedFile, EmptyLoadTruncatesExistingFileWithoutInvokingReader) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("old contents", 12, 0), 0);
  bool reader_called = false;
  ASSERT_EQ(file.load(0, [&](uint64_t, uint64_t) {
    reader_called = true;
    return std::optional<std::vector<uint8_t>>{};
  }), 0);
  EXPECT_FALSE(reader_called);
  EXPECT_EQ(file.size(), 0u);
  char byte = '!';
  EXPECT_EQ(file.read(&byte, 1, 0), 0);
  EXPECT_EQ(byte, '!');
}

TEST(StagedFile, ReloadWithShorterDataRemovesPreviousTail) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("previous data", 13, 0), 0);
  ASSERT_EQ(file.load(3, [](uint64_t offset, uint64_t length) {
    EXPECT_EQ(offset, 0u);
    EXPECT_EQ(length, 3u);
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{'n', 'e', 'w'});
  }), 0);
  EXPECT_EQ(file.size(), 3u);
  char bytes[13]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 3);
  EXPECT_EQ(std::string(bytes, 3), "new");
}

TEST(StagedFile, LoadRejectsAbsentAndOversizedReaderResults) {
  iotdbfs::StagedFile absent;
  EXPECT_EQ(absent.load(8, [](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>{};
  }), -EIO);
  iotdbfs::StagedFile oversized;
  EXPECT_EQ(oversized.load(8, [](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>(9));
  }), -EIO);
}

TEST(StagedFile, LoadStopsImmediatelyAfterReaderFailure) {
  iotdbfs::StagedFile file;
  unsigned calls = 0;
  EXPECT_EQ(file.load(3 * 1024 * 1024, [&](uint64_t, uint64_t length) {
    ++calls;
    if (calls == 2)
      return std::optional<std::vector<uint8_t>>{};
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>(length, 0x5a));
  }), -EIO);
  EXPECT_EQ(calls, 2u);
}

TEST(StagedFile, LoadPreservesDataAcrossTransferChunkBoundary) {
  iotdbfs::StagedFile file;
  constexpr uint64_t chunk = 1024 * 1024;
  uint64_t next_offset = 0;
  ASSERT_EQ(file.load(chunk + 7, [&](uint64_t offset, uint64_t length) {
    EXPECT_EQ(offset, next_offset);
    EXPECT_EQ(length, offset == 0 ? chunk : 7u);
    next_offset += length;
    std::vector<uint8_t> bytes(length);
    for (uint64_t i = 0; i < length; ++i)
      bytes[i] = static_cast<uint8_t>((offset + i) % 251);
    return std::optional<std::vector<uint8_t>>(std::move(bytes));
  }), 0);
  std::array<uint8_t, 16> bytes{};
  ASSERT_EQ(file.read(bytes.data(), bytes.size(), chunk - 9), 16);
  for (size_t i = 0; i < bytes.size(); ++i)
    EXPECT_EQ(bytes[i], (chunk - 9 + i) % 251);
  EXPECT_EQ(next_offset, chunk + 7);
}

TEST(StagedFile, WriteRangeOverflowPreservesExistingSizeAndData) {
  iotdbfs::StagedFile file;
  ASSERT_EQ(file.create(), 0);
  ASSERT_EQ(file.write("abc", 3, 0), 0);
  const auto limit = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
  EXPECT_EQ(file.write("xy", 2, limit - 1), -EFBIG);
  EXPECT_EQ(file.write("x", 1, limit), -EFBIG);
  EXPECT_EQ(file.resize(UINT64_MAX), -EFBIG);
  EXPECT_EQ(file.size(), 3u);
  char bytes[3]{};
  ASSERT_EQ(file.read(bytes, sizeof(bytes), 0), 3);
  EXPECT_EQ(std::string(bytes, sizeof(bytes)), "abc");
}
