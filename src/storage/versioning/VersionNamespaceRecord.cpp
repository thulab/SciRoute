#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include <limits>

namespace iotdbfs::versioning {
namespace {
void require(bool okay) { if (!okay) throw Error(ErrorCode::Corrupt, "invalid namespace record"); }
struct Encoder {
  Bytes bytes;
  void number(std::uint64_t n) { for (int i=7;i>=0;--i) bytes.push_back(static_cast<std::uint8_t>(n>>(i*8))); }
  void text(const std::string& s) { require(s.size()<=65536); number(s.size()); bytes.insert(bytes.end(),s.begin(),s.end()); }
};
struct Decoder {
  const Bytes& bytes; std::size_t pos{0};
  std::uint64_t number() { require(bytes.size()-pos>=8); std::uint64_t n=0;for(int i=0;i<8;++i)n=(n<<8)|bytes[pos++];return n; }
  std::string text() { auto n=number();require(n<=65536&&n<=bytes.size()-pos);std::string s(bytes.begin()+pos,bytes.begin()+pos+n);pos+=n;return s; }
  long long signed_number() { auto n=number();require(n<=static_cast<std::uint64_t>(std::numeric_limits<long long>::max()));return static_cast<long long>(n); }
  int integer() {auto n=number();require(n<=static_cast<std::uint64_t>(std::numeric_limits<int>::max()));return static_cast<int>(n);}
};
void validate(const NamespaceRecord& r) {
  // Share the exact-binding grammar: absolute, canonical components, no NUL.
  // PathUtils::norm only supplies a leading slash and is not a validator.
  try { (void)exact_key(r.inode.path, r.inode.row_time); }
  catch (const Error&) { require(false); }
  require(r.inode.name==path_utils::base(r.inode.path));
  require(r.inode.parent==path_utils::parent_of(r.inode.path)&&r.inode.row_time>=0&&r.inode.size>=0);
  require(r.inode.mode>=0&&r.inode.uid>=0&&r.inode.gid>=0&&r.inode.nlink>=0);
  require(r.inode.ctime>=0&&r.inode.mtime>=0&&r.inode.atime>=0);
  require(r.manifest.key.path==r.inode.path&&r.manifest.key.version==r.inode.row_time);
  require(r.manifest.published&&r.manifest.phase==StructuredBlobPhase::Published);
  require(r.manifest.payload_size==static_cast<std::uint64_t>(r.inode.size));
  require(r.manifest.format_id.value_or("")==r.inode.format_id);
  if (r.inode.is_dir) require(r.descriptor_id.empty() && r.seal_id.empty());
  else require(valid_content_id(r.descriptor_id) && valid_content_id(r.seal_id));
}
}
Bytes encode_namespace_record(const NamespaceRecord& r) {
  validate(r);
  std::string inline_metadata;
  if (r.inode.is_dir) {
    auto im=deserialize_metadata(r.inode.metadata);require(im.ok());
    auto metadata=serialize_metadata(r.manifest.metadata,65536);
    auto inode_metadata=serialize_metadata(im.value(),65536);
    require(metadata.ok()&&inode_metadata.ok()&&metadata.value()==inode_metadata.value());
    inline_metadata=metadata.value();
  }
  Encoder e;e.text("iotdbfs-namespace-record/3");e.text(r.inode.path);e.text(r.inode.parent);e.text(r.inode.name);
  e.number(r.inode.row_time);e.number(r.inode.is_dir);e.number(r.inode.size);e.number(r.inode.mode);e.number(r.inode.uid);e.number(r.inode.gid);e.number(r.inode.nlink);
  e.number(r.inode.ctime);e.number(r.inode.mtime);e.number(r.inode.atime);e.text(r.inode.format_id);e.text(inline_metadata);e.number(r.manifest.layout_size);e.number(r.manifest.revision);
  e.text(r.descriptor_id);e.text(r.seal_id);
  return std::move(e.bytes);
}
NamespaceRecord decode_namespace_record(const Bytes& bytes) {
  require(bytes.size()<=kMetadataBytes);Decoder d{bytes};require(d.text()=="iotdbfs-namespace-record/3");NamespaceRecord r;
  r.inode.path=d.text();r.inode.parent=d.text();r.inode.name=d.text();r.inode.row_time=d.signed_number();auto dir=d.number();require(dir<=1);r.inode.is_dir=dir;
  r.inode.size=d.signed_number();r.inode.mode=d.integer();r.inode.uid=d.integer();r.inode.gid=d.integer();r.inode.nlink=d.integer();r.inode.ctime=d.signed_number();r.inode.mtime=d.signed_number();r.inode.atime=d.signed_number();r.inode.format_id=d.text();r.inode.metadata=d.text();
  r.manifest.key={r.inode.path,r.inode.row_time};r.manifest.payload_size=r.inode.size;r.manifest.layout_size=d.number();r.manifest.revision=d.number();r.descriptor_id=d.text();r.seal_id=d.text();r.manifest.published=true;r.manifest.phase=StructuredBlobPhase::Published;
  if(!r.inode.format_id.empty())r.manifest.format_id=r.inode.format_id;
  if (r.inode.is_dir) {
    auto metadata=deserialize_metadata(r.inode.metadata);require(metadata.ok());
    r.manifest.metadata=std::move(metadata.value());
  } else require(r.inode.metadata.empty());
  require(d.pos==bytes.size());validate(r);require(encode_namespace_record(r)==bytes);return r;
}
} // namespace iotdbfs::versioning
