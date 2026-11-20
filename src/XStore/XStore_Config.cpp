#include "XStore/XStore_Config.h"

#include "XStore/XStore_Crc32c.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>

#include <fcntl.h>
#include <unistd.h>

namespace xstore::config {
namespace {

constexpr char MAGIC[4] = {'X', 'S', 'C', 'F'};
constexpr size_t FRAME_HEAD = 4 + 2 + 4;   // magic, version, body_len
constexpr size_t FRAME_TAIL = 4;           // crc

void PutU32(std::string &out, uint32_t v) { out.append(reinterpret_cast<const char *>(&v), 4); }
void PutStr(std::string &out, const std::string &s) {
    PutU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}

class BodyReader {
public:
    explicit BodyReader(std::string_view bytes) : bytes_(bytes) {}
    bool U8(uint8_t *v) {
        if (bytes_.size() - at_ < 1) return false;
        *v = static_cast<uint8_t>(bytes_[at_++]);
        return true;
    }
    bool U32(uint32_t *v) {
        if (bytes_.size() - at_ < 4) return false;
        std::memcpy(v, bytes_.data() + at_, 4);
        at_ += 4;
        return true;
    }
    bool Str(std::string *s) {
        uint32_t len;
        if (!U32(&len) || bytes_.size() - at_ < len) return false;
        s->assign(bytes_.data() + at_, len);
        at_ += len;
        return true;
    }
    [[nodiscard]] bool AtEnd() const { return at_ == bytes_.size(); }

private:
    std::string_view bytes_;
    size_t at_ = 0;
};

}  // namespace

const char *StatusMessage(LoadStatus status) {
    switch (status) {
        case LoadStatus::kOk: return "ok";
        case LoadStatus::kMissing: return "the config file is missing";
        case LoadStatus::kOldFormat: return "old-format database (a protobuf .CFG from an older server): recreate it";
        case LoadStatus::kCorrupt: return "the config file is corrupt (truncated or checksum mismatch)";
        case LoadStatus::kUnsupportedVersion: return "the config file was written by a newer server version";
    }
    return "unknown";
}

std::string Serialize(const XStore_Proto::ConfigStruct &cfg) {
    std::string body;
    PutU32(body, static_cast<uint32_t>(cfg.timeresolution()));
    PutU32(body, static_cast<uint32_t>(cfg.rowlength()));
    PutU32(body, cfg.columncount());
    PutStr(body, cfg.dbname());
    PutU32(body, static_cast<uint32_t>(cfg.columnnames_size()));
    for (const std::string &name : cfg.columnnames()) PutStr(body, name);
    PutU32(body, static_cast<uint32_t>(cfg.schema_size()));
    for (const XStore_Proto::ColumnSpec &spec : cfg.schema()) {
        body.push_back(static_cast<char>(spec.kind()));
        body.push_back(static_cast<char>(spec.storage()));
        body.push_back(static_cast<char>(spec.capacity()));
    }
    std::string out(MAGIC, 4);
    const uint16_t version = FORMAT_VERSION;
    out.append(reinterpret_cast<const char *>(&version), 2);
    PutU32(out, static_cast<uint32_t>(body.size()));
    out.append(body);
    PutU32(out, Crc32c(body.data(), body.size()));
    return out;
}

LoadStatus Parse(std::string_view bytes, XStore_Proto::ConfigStruct *out) {
    // A file shorter than the magic that is a prefix of it is an XSCF file cut short (corrupt); an empty file or any other short
    // file is what an older server's tiny protobuf .CFG looks like.
    if (!bytes.empty() && bytes.size() < 4 && std::memcmp(bytes.data(), MAGIC, bytes.size()) == 0) return LoadStatus::kCorrupt;
    if (bytes.size() < 4 || std::memcmp(bytes.data(), MAGIC, 4) != 0) return LoadStatus::kOldFormat;
    if (bytes.size() < FRAME_HEAD + FRAME_TAIL) return LoadStatus::kCorrupt;
    uint16_t version;
    std::memcpy(&version, bytes.data() + 4, 2);
    if (version != FORMAT_VERSION) return LoadStatus::kUnsupportedVersion;
    uint32_t body_len;
    std::memcpy(&body_len, bytes.data() + 6, 4);
    if (bytes.size() != FRAME_HEAD + static_cast<size_t>(body_len) + FRAME_TAIL) return LoadStatus::kCorrupt;
    const std::string_view body = bytes.substr(FRAME_HEAD, body_len);
    uint32_t crc;
    std::memcpy(&crc, bytes.data() + FRAME_HEAD + body_len, 4);
    if (crc != Crc32c(body.data(), body.size())) return LoadStatus::kCorrupt;

    BodyReader in(body);
    uint32_t time_resolution, row_length, column_count, name_count, schema_count;
    std::string db_name;
    if (!in.U32(&time_resolution) || !in.U32(&row_length) || !in.U32(&column_count) || !in.Str(&db_name) || !in.U32(&name_count)) return LoadStatus::kCorrupt;
    XStore_Proto::ConfigStruct cfg;
    cfg.set_dbname(db_name);
    cfg.set_timeresolution(static_cast<XStore_Proto::TimeUnit>(time_resolution));
    cfg.set_rowlength(static_cast<int32_t>(row_length));
    cfg.set_columncount(column_count);
    for (uint32_t i = 0; i < name_count; ++i) {
        std::string name;
        if (!in.Str(&name)) return LoadStatus::kCorrupt;
        cfg.add_columnnames(std::move(name));
    }
    if (!in.U32(&schema_count)) return LoadStatus::kCorrupt;
    for (uint32_t i = 0; i < schema_count; ++i) {
        uint8_t kind, storage, capacity;
        if (!in.U8(&kind) || !in.U8(&storage) || !in.U8(&capacity)) return LoadStatus::kCorrupt;
        XStore_Proto::ColumnSpec *spec = cfg.add_schema();
        spec->set_kind(kind);
        spec->set_storage(storage);
        spec->set_capacity(capacity);
    }
    if (!in.AtEnd()) return LoadStatus::kCorrupt;
    *out = std::move(cfg);
    return LoadStatus::kOk;
}

LoadStatus Load(const std::string &path, XStore_Proto::ConfigStruct *out) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return LoadStatus::kMissing;
    const std::string bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    return Parse(bytes, out);
}

bool Save(const std::string &path, const XStore_Proto::ConfigStruct &cfg, std::string *error) {
    auto fail = [&](const char *what) {
        if (error != nullptr) *error = std::string(what) + ": " + std::strerror(errno);
        return false;
    };
    const std::string bytes = Serialize(cfg);
    const std::string tmp = path + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return fail("open the temporary config file");
    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            const int saved = errno;
            ::close(fd);
            errno = saved;
            return fail("write the temporary config file");
        }
        written += static_cast<size_t>(n);
    }
    if (::fsync(fd) != 0) {
        const int saved = errno;
        ::close(fd);
        errno = saved;
        return fail("fsync the temporary config file");
    }
    ::close(fd);
    if (::rename(tmp.c_str(), path.c_str()) != 0) return fail("rename the temporary config file");
    const size_t slash = path.find_last_of('/');
    const std::string dir = slash == std::string::npos ? std::string(".") : path.substr(0, slash);
    const int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0) return fail("open the config directory");
    const bool synced = ::fsync(dir_fd) == 0;
    const int saved = errno;
    ::close(dir_fd);
    errno = saved;
    return synced ? true : fail("fsync the config directory");
}

}  // namespace xstore::config
