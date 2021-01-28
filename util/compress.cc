#include "util/compress.hh"

#include "util/file.hh"
#include "util/have.hh"
#include "util/scoped.hh"

#include <algorithm>
#include <iostream>

#include <cassert>
#include <climits>
#include <cstdlib>
#include <cstring>

#ifdef HAVE_ZLIB
#include <zlib.h>
#endif

#ifdef HAVE_BZLIB
#include <bzlib.h>
#endif

#ifdef HAVE_XZLIB
#include <lzma.h>
#endif

namespace util {

CompressedException::CompressedException() throw() {}
CompressedException::~CompressedException() throw() {}

GZException::GZException() throw() {}
GZException::~GZException() throw() {}

BZException::BZException() throw() {}
BZException::~BZException() throw() {}

XZException::XZException() throw() {}
XZException::~XZException() throw() {}

void ReadBase::ReplaceThis(ReadBase *with, ReadCompressed &thunk) {
  thunk.internal_.reset(with);
}

ReadBase *ReadBase::Current(ReadCompressed &thunk) { return thunk.internal_.get(); }

uint64_t &ReadBase::ReadCount(ReadCompressed &thunk) {
  return thunk.raw_amount_;
}

std::size_t ReadBase::Skip(ReadCompressed &thunk) {
  UTIL_THROW(util::Exception, "ReadBase::Skip is not implemented");
  return 0;
}

std::size_t ReadBase::SkipTo(std::vector<std::size_t> const &offsets, ReadCompressed &thunk) {
  UTIL_THROW(util::Exception, "ReadBase::SkipTo is not implemented");
  return 0;
}

namespace {

ReadBase *ReadFactory(int fd, uint64_t &raw_amount, const void *already_data, std::size_t already_size, bool require_compressed);

// Completed file that other classes can thunk to.
class Complete : public ReadBase {
  public:
    std::size_t Read(void *, std::size_t, ReadCompressed &) {
      return 0;
    }
};

class Uncompressed : public ReadBase {
  public:
    explicit Uncompressed(int fd) : fd_(fd) {}

    std::size_t Read(void *to, std::size_t amount, ReadCompressed &thunk) {
      std::size_t got = PartialRead(fd_.get(), to, amount);
      ReadCount(thunk) += got;
      return got;
    }

  private:
    scoped_fd fd_;
};

class UncompressedWithHeader : public ReadBase {
  public:
    UncompressedWithHeader(int fd, const void *already_data, std::size_t already_size) : fd_(fd) {
      assert(already_size);
      buf_.reset(malloc(already_size));
      if (!buf_.get()) throw std::bad_alloc();
      memcpy(buf_.get(), already_data, already_size);
      remain_ = static_cast<uint8_t*>(buf_.get());
      end_ = remain_ + already_size;
    }

    std::size_t Read(void *to, std::size_t amount, ReadCompressed &thunk) {
      assert(buf_.get());
      assert(remain_ != end_);
      std::size_t sending = std::min<std::size_t>(amount, end_ - remain_);
      memcpy(to, remain_, sending);
      remain_ += sending;
      if (remain_ == end_) {
        ReplaceThis(new Uncompressed(fd_.release()), thunk);
      }
      return sending;
    }

  private:
    scoped_malloc buf_;
    uint8_t *remain_;
    uint8_t *end_;

    scoped_fd fd_;
};

static const std::size_t kInputBuffer = 16384;

template <class Compression> class StreamCompressed : public ReadBase {
  public:
    StreamCompressed(int fd, const void *already_data, std::size_t already_size)
      : file_(fd),
        in_buffer_(MallocOrThrow(kInputBuffer)),
        back_(memcpy(in_buffer_.get(), already_data, already_size), already_size) {}

    std::size_t Read(void *to, std::size_t amount, ReadCompressed &thunk) {
      if (amount == 0) return 0;
      back_.SetOutput(to, amount);
      do {
        if (!back_.Stream().avail_in) ReadInput(thunk);
        if (!back_.Process()) {
          // reached end, at least for the compressed portion.
          std::size_t ret = static_cast<const uint8_t *>(static_cast<void*>(back_.Stream().next_out)) - static_cast<const uint8_t*>(to);
          ReplaceThis(ReadFactory(file_.release(), ReadCount(thunk), back_.Stream().next_in, back_.Stream().avail_in, true), thunk);
          if (ret) return ret;
          // We did not read anything this round, so clients might think EOF.  Transfer responsibility to the next reader.
          return Current(thunk)->Read(to, amount, thunk);
        }
      } while (back_.Stream().next_out == to);
      return static_cast<const uint8_t*>(static_cast<void*>(back_.Stream().next_out)) - static_cast<const uint8_t*>(to);
    }

    // Searches for next magic header
    std::size_t Skip(ReadCompressed &thunk) {
      std::size_t skipped = 0;
      while (back_.Stream().avail_in) {
        std::size_t offset = FindMagic(static_cast<const void*>(back_.Stream().next_in), back_.Stream().avail_in);
        skipped += offset;

        back_.SetInput(back_.Stream().next_in + offset, back_.Stream().avail_in - offset);

        // if we scanned till the end
        if (!back_.Stream().avail_in) {
          // read ahead, but keep the last 5 bytes to catch magic spanning two reads. (8 for alignment?)
          ReadInput(thunk, back_.Stream().next_in - 8, 8);
          continue;
        }
        
        ReplaceThis(ReadFactory(file_.release(), ReadCount(thunk), back_.Stream().next_in, back_.Stream().avail_in, true), thunk);
        break;
      }
      return skipped;
    }

    std::size_t SkipTo(std::vector<std::size_t> const &offsets, ReadCompressed &thunk) {
      uint64_t pos = ReadCount(thunk) - back_.Stream().avail_in;
      uint64_t offset = 0;
      for (auto it = offsets.begin(); it != offsets.end(); ++it) {
        if (*it > pos) {
          offset = *it;
          break;
        }
      }

      UTIL_THROW_IF(!offset, util::CompressedException, "No jump target beyond " << pos << " in offset list");

      // Is the jump target already in our buffer, or is it further away?
      if (offset < ReadCount(thunk)) {
        back_.SetInput(back_.Stream().next_in + (offset - pos), back_.Stream().avail_in - (offset - pos));
      } else {
        back_.SetInput(nullptr, 0);

        // Read into our buffer until we reach the target. We don't do anything with the buffer,
        // i.e. we could also have used a ForwardSeek function here. Loop because we can only read
        // one buffer at a time.
        while (ReadCount(thunk) != offset) {
          std::size_t got = ReadOrEOF(file_.get(), in_buffer_.get(), std::min(kInputBuffer, static_cast<std::size_t>(offset - ReadCount(thunk))));
          if (!got)
            break;
          ReadCount(thunk) += got;
        }
      }

      // Start reading a new from here.
      ReplaceThis(ReadFactory(file_.release(), ReadCount(thunk), back_.Stream().next_in, back_.Stream().avail_in, true), thunk);
      return (ReadCount(thunk) - back_.Stream().avail_in) - pos;
    }

  private:
    std::size_t FindMagic(const void * data, std::size_t size) {
      const uint8_t kXZMagic[6] = { 0xFD, '7', 'z', 'X', 'Z', 0x00 };
      const uint8_t *next = static_cast<const uint8_t*>(memmem(static_cast<const uint8_t*>(data), size, kXZMagic, sizeof(kXZMagic)));
      if (next == nullptr)
        return size;
      else
        return next - static_cast<const uint8_t*>(data);
    }

    void ReadInput(ReadCompressed &thunk, const void *already_data, std::size_t already_size) {
      assert(!back_.Stream().avail_in);
      if (already_size != 0) {
        assert(already_size < kInputBuffer);
        std::memcpy(in_buffer_.get(), already_data, already_size);
      }
      std::size_t got = ReadOrEOF(file_.get(), static_cast<void*>(static_cast<char*>(in_buffer_.get()) + already_size), kInputBuffer - already_size);
      back_.SetInput(in_buffer_.get(), already_size + got);
      ReadCount(thunk) += got;
    }

    void ReadInput(ReadCompressed &thunk) {
      ReadInput(thunk, nullptr, 0);
    }

    scoped_fd file_;
    scoped_malloc in_buffer_;

    Compression back_;
};

#ifdef HAVE_ZLIB
class GZip {
  public:
    GZip() {
      stream_.zalloc = Z_NULL;
      stream_.zfree = Z_NULL;
      stream_.opaque = Z_NULL;
      stream_.msg = NULL;
    }

    void SetOutput(void *to, std::size_t amount) {
      stream_.next_out = static_cast<Bytef*>(to);
      stream_.avail_out = std::min<std::size_t>(std::numeric_limits<uInt>::max(), amount);
    }

    void SetInput(const void *base, std::size_t amount) {
      assert(amount < static_cast<std::size_t>(std::numeric_limits<uInt>::max()));
      stream_.next_in = const_cast<Bytef*>(static_cast<const Bytef*>(base));
      stream_.avail_in = amount;
    }

    const z_stream &Stream() const { return stream_; }

  protected:
    z_stream stream_;
};

class GZipRead : public GZip {
  public:
    GZipRead(const void *base, std::size_t amount) {
      SetInput(base, amount);
      // 32 for zlib and gzip decoding with automatic header detection.
      // 15 for maximum window size.
      UTIL_THROW_IF(Z_OK != inflateInit2(&stream_, 32 + 15), GZException, "Failed to initialize zlib.");
    }

    ~GZipRead() {
      if (Z_OK != inflateEnd(&stream_)) {
        std::cerr << "zlib could not close properly." << std::endl;
        abort();
      }
    }

    bool Process() {
      int result = inflate(&stream_, 0);
      switch (result) {
        case Z_OK:
          return true;
        case Z_STREAM_END:
          return false;
        case Z_ERRNO:
          UTIL_THROW(ErrnoException, "zlib error");
        default:
          UTIL_THROW(GZException, "zlib encountered " << (stream_.msg ? stream_.msg : "an error ") << " code " << result);
      }
    }
};

class GZipWrite : public GZip {
  public:
    explicit GZipWrite(int level) {
      UTIL_THROW_IF(Z_OK != deflateInit2(
            &stream_,
            level,
            Z_DEFLATED,
            16 /* gzip support */ + 15 /* default window */,
            8 /* default */,
            Z_DEFAULT_STRATEGY), GZException, "Failed to initialize zlib decompression.");
    }

    ~GZipWrite() {
      deflateEnd(&stream_);
    }

    void Process() {
      int result = deflate(&stream_, Z_NO_FLUSH);
      UTIL_THROW_IF(Z_OK != result, GZException, "zlib encountered " << (stream_.msg ? stream_.msg : "an error ") << " code " << result);
    }

    bool Finish() {
      UTIL_THROW_IF(!stream_.avail_out, Exception, "No available output.");
      int result = deflate(&stream_, Z_FINISH);
      switch (result) {
        case Z_STREAM_END:
          return true;
        case Z_OK:
          return false;
        // "If deflate returns with Z_OK or Z_BUF_ERROR, this function must be called again with Z_FINISH and more output space"
        case Z_BUF_ERROR:
          return false;
        default:
          UTIL_THROW(GZException, "zlib encountered " << (stream_.msg ? stream_.msg : "an error ") << " code " << result);
      }
    }
};
#endif // HAVE_ZLIB

#ifdef HAVE_BZLIB
class BZip {
  public:
    BZip(const void *base, std::size_t amount) {
      memset(&stream_, 0, sizeof(stream_));
      SetInput(base, amount);
      HandleError(BZ2_bzDecompressInit(&stream_, 0, 0));
    }

    ~BZip() {
      try {
        HandleError(BZ2_bzDecompressEnd(&stream_));
      } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        abort();
      }
    }

    bool Process() {
      int ret = BZ2_bzDecompress(&stream_);
      if (ret == BZ_STREAM_END) return false;
      HandleError(ret);
      return true;
    }

    void SetOutput(void *base, std::size_t amount) {
      stream_.next_out = static_cast<char*>(base);
      stream_.avail_out = std::min<std::size_t>(std::numeric_limits<unsigned int>::max(), amount);
    }

    void SetInput(const void *base, std::size_t amount) {
      stream_.next_in = const_cast<char*>(static_cast<const char*>(base));
      stream_.avail_in = amount;
    }

    const bz_stream &Stream() const { return stream_; }

  private:
    void HandleError(int value) {
      switch(value) {
        case BZ_OK:
          return;
        case BZ_CONFIG_ERROR:
          UTIL_THROW(BZException, "bzip2 seems to be miscompiled.");
        case BZ_PARAM_ERROR:
          UTIL_THROW(BZException, "bzip2 Parameter error");
        case BZ_DATA_ERROR:
          UTIL_THROW(BZException, "bzip2 detected a corrupt file");
        case BZ_DATA_ERROR_MAGIC:
          UTIL_THROW(BZException, "bzip2 detected bad magic bytes.  Perhaps this was not a bzip2 file after all?");
        case BZ_MEM_ERROR:
          throw std::bad_alloc();
        default:
          UTIL_THROW(BZException, "Unknown bzip2 error code " << value);
      }
    }

    bz_stream stream_;
};
#endif // HAVE_BZLIB

#ifdef HAVE_XZLIB
class XZip {
  public:
    XZip(const void *base, std::size_t amount)
      : stream_(LZMA_STREAM_INIT), action_(LZMA_RUN) {
      SetInput(base, amount);
      HandleError(lzma_stream_decoder(&stream_, UINT64_MAX, 0));
    }

    ~XZip() {
      lzma_end(&stream_);
    }

    void SetOutput(void *base, std::size_t amount) {
      stream_.next_out = static_cast<uint8_t*>(base);
      stream_.avail_out = amount;
    }

    void SetInput(const void *base, std::size_t amount) {
      stream_.next_in = static_cast<const uint8_t*>(base);
      stream_.avail_in = amount;
      if (!amount) action_ = LZMA_FINISH;
    }

    const lzma_stream &Stream() const { return stream_; }

    bool Process() {
      lzma_ret status = lzma_code(&stream_, action_);
      if (status == LZMA_STREAM_END) return false;
      HandleError(status);
      return true;
    }

  private:
    void HandleError(lzma_ret value) {
      switch (value) {
        case LZMA_OK:
          return;
        case LZMA_MEM_ERROR:
          throw std::bad_alloc();
        case LZMA_FORMAT_ERROR:
          UTIL_THROW(XZException, "xzlib says file format not recognized");
        case LZMA_OPTIONS_ERROR:
          UTIL_THROW(XZException, "xzlib says unsupported compression options");
        case LZMA_DATA_ERROR:
          UTIL_THROW(XZException, "xzlib says this file is corrupt");
        case LZMA_BUF_ERROR:
          UTIL_THROW(XZException, "xzlib says unexpected end of input");
        default:
          UTIL_THROW(XZException, "unrecognized xzlib error " << value);
      }
    }

    lzma_stream stream_;
    lzma_action action_;
};
#endif // HAVE_XZLIB

class IStreamReader : public ReadBase {
  public:
    explicit IStreamReader(std::istream &stream) : stream_(stream) {}

    std::size_t Read(void *to, std::size_t amount, ReadCompressed &thunk) {
      if (!stream_.read(static_cast<char*>(to), amount)) {
        UTIL_THROW_IF(!stream_.eof(), ErrnoException, "istream error");
        amount = stream_.gcount();
      }
      ReadCount(thunk) += amount;
      return amount;
    }

  private:
    std::istream &stream_;
};

enum MagicResult {
  UTIL_UNKNOWN, UTIL_GZIP, UTIL_BZIP, UTIL_XZIP
};

MagicResult DetectMagic(const void *from_void, std::size_t length) {
  const uint8_t *header = static_cast<const uint8_t*>(from_void);
  if (length >= 2 && header[0] == 0x1f && header[1] == 0x8b) {
    return UTIL_GZIP;
  }
  const uint8_t kBZMagic[3] = {'B', 'Z', 'h'};
  if (length >= sizeof(kBZMagic) && !memcmp(header, kBZMagic, sizeof(kBZMagic))) {
    return UTIL_BZIP;
  }
  const uint8_t kXZMagic[6] = { 0xFD, '7', 'z', 'X', 'Z', 0x00 };
  if (length >= sizeof(kXZMagic) && !memcmp(header, kXZMagic, sizeof(kXZMagic))) {
    return UTIL_XZIP;
  }
  return UTIL_UNKNOWN;
}

ReadBase *ReadFactory(int fd, uint64_t &raw_amount, const void *already_data, const std::size_t already_size, bool require_compressed) {
  scoped_fd hold(fd);
  std::string header(reinterpret_cast<const char*>(already_data), already_size);
  if (header.size() < ReadCompressed::kMagicSize) {
    std::size_t original = header.size();
    header.resize(ReadCompressed::kMagicSize);
    std::size_t got = ReadOrEOF(fd, &header[original], ReadCompressed::kMagicSize - original);
    raw_amount += got;
    header.resize(original + got);
  }
  if (header.empty()) {
    return new Complete();
  }
  switch (DetectMagic(&header[0], header.size())) {
    case UTIL_GZIP:
#ifdef HAVE_ZLIB
      return new StreamCompressed<GZipRead>(hold.release(), header.data(), header.size());
#else
      UTIL_THROW(CompressedException, "This looks like a gzip file but gzip support was not compiled in.");
#endif
    case UTIL_BZIP:
#ifdef HAVE_BZLIB
      return new StreamCompressed<BZip>(hold.release(), &header[0], header.size());
#else
      UTIL_THROW(CompressedException, "This looks like a bzip file (it begins with BZh), but bzip support was not compiled in.");
#endif
    case UTIL_XZIP:
#ifdef HAVE_XZLIB
      return new StreamCompressed<XZip>(hold.release(), header.data(), header.size());
#else
      UTIL_THROW(CompressedException, "This looks like an xz file, but xz support was not compiled in.");
#endif
    default:
      UTIL_THROW_IF(require_compressed, CompressedException, "Uncompressed data detected after a compresssed file.  This could be supported but usually indicates an error.");
      return new UncompressedWithHeader(hold.release(), header.data(), header.size());
  }
}

} // namespace

bool ReadCompressed::DetectCompressedMagic(const void *from_void) {
  return DetectMagic(from_void, kMagicSize) != UTIL_UNKNOWN;
}

ReadCompressed::ReadCompressed(int fd) {
  Reset(fd);
}

ReadCompressed::ReadCompressed(std::istream &in) {
  Reset(in);
}

ReadCompressed::ReadCompressed() {}

void ReadCompressed::Reset(int fd) {
  raw_amount_ = 0;
  internal_.reset();
  internal_.reset(ReadFactory(fd, raw_amount_, NULL, 0, false));
}

void ReadCompressed::Reset(std::istream &in) {
  internal_.reset();
  internal_.reset(new IStreamReader(in));
}

std::size_t ReadCompressed::Read(void *to, std::size_t amount) {
  return internal_->Read(to, amount, *this);
}

std::size_t ReadCompressed::Skip() {
  return internal_->Skip(*this);
}

std::size_t ReadCompressed::SkipTo(std::vector<std::size_t> const &offsets) {
  return internal_->SkipTo(offsets, *this);
}

std::size_t ReadCompressed::ReadOrEOF(void *const to_in, std::size_t amount) {
  uint8_t *to = reinterpret_cast<uint8_t*>(to_in);
  while (amount) {
    std::size_t got = Read(to, amount);
    if (!got) break;
    to += got;
    amount -= got;
  }
  return to - reinterpret_cast<uint8_t*>(to_in);
}

#ifdef HAVE_ZLIB
namespace {

void EnsureOutput(GZipWrite &writer, std::string &to) {
  const std::size_t kIncrement = 4096;
  if (writer.Stream().avail_out < 6 /* magic number in zlib.h to avoid multiple ends */) {
    std::size_t old_done = reinterpret_cast<const char*>(writer.Stream().next_out) - to.data();
    to.resize(to.size() + kIncrement);
    writer.SetOutput(&to[old_done], to.size() - old_done);
  }
}

} // namespace

void GZCompress(StringPiece from, std::string &to, int level) {
  to.clear();
  to.resize(4096);
  GZipWrite writer(level);
  writer.SetInput(from.data(), from.size());
  writer.SetOutput(&to[0], to.size());
  while (writer.Stream().avail_in) {
    EnsureOutput(writer, to);
    writer.Process();
  }
  do {
    EnsureOutput(writer, to);
  } while (!writer.Finish());
  to.resize(reinterpret_cast<const char*>(writer.Stream().next_out) - to.data());
}
#else
void GZCompress(StringPiece, std::string &, int) {
  UTIL_THROW(CompressedException, "GZip support was not compiled in.");
}
#endif

} // namespace util
