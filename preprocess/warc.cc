#include "preprocess/warc.hh"

#include "util/exception.hh"
#include "util/file.hh"
#include "util/file_piece.hh"
#include "util/compress.hh"

#include <cstdlib>
#include <limits>
#include <string>
#include <strings.h>
#include <iostream>

namespace preprocess {

WARCReadException::WARCReadException() throw() {}
WARCReadException::~WARCReadException() throw() {}

bool ReadMore(util::ReadCompressed &reader, std::string &out) {
  const std::size_t kRead = 4096;
  std::size_t had = out.size();
  out.resize(out.size() + kRead);
  std::size_t got = reader.Read(&out[had], out.size() - had);
  if (!got) {
    // End of file
    UTIL_THROW_IF(had, util::EndOfFileException, "Unexpected end of file inside header");
    return false;
  }
  out.resize(had + got);
  return true;
}

class HeaderReader {
  public:
    HeaderReader(util::ReadCompressed &reader, std::string &out)
      : reader_(reader), out_(out), consumed_(0) {}

    bool Line(util::StringPiece &line) {
      std::size_t newline_start = consumed_;
      std::size_t newline;
      while (std::string::npos == (newline = out_.find('\n', newline_start))) {
        newline_start = out_.size();
        if (!ReadMore(reader_, out_)) return false;
      }
      // The line is [consumed, newline).  A blank line indicates header end.
      line = util::StringPiece(out_.data() + consumed_, newline - consumed_);
      // Remove carriage return if present.
      if (!line.empty() && line.data()[line.size() - 1] == '\r') {
        line = util::StringPiece(line.data(), line.size() - 1);
      }
      consumed_ = newline + 1;
      return true;
    }

    std::size_t Consumed() const { return consumed_; }

  private:
    util::ReadCompressed &reader_;
    std::string &out_;

    std::size_t consumed_;
};

WARCReader::WARCReader(std::string const &filename)
: WARCReader(util::OpenReadOrThrow(filename.c_str())) {
  try {
    // replace the .warc.whatever extension with .txt
    std::size_t end = filename.rfind(".warc.");
    std::string index_filename = filename.substr(0, end) + ".txt";
    util::FilePiece index_fh(index_filename.c_str());
    for (auto line : index_fh)
      offsets_.push_back(std::atoll(line.data()));
    std::cerr << "Found index file " << index_filename << " with " << offsets_.size() << " offsets" << std::endl;
  } catch (util::ErrnoException const &e) {
    std::cerr << "No offsets available for " << filename << ": " << e.what() << std::endl;
  }
}

bool WARCReader::Read(Record &out, std::size_t size_limit) {
  std::swap(overhang_, out.str);
  overhang_.clear();
  out.skipped = 0;
  out.str.reserve(32768);
  HeaderReader header(reader_, out.str);
  util::StringPiece line;
  try {
    if (!header.Line(line)) return false;
    UTIL_THROW_IF(line != "WARC/1.0", WARCReadException, reader_ << "Expected WARC/1.0 header but got `" << line << '\'');
    std::size_t length = 0;
    bool seen_content_length = false;
    const char kContentLength[] = "Content-Length:";
    const std::size_t kContentLengthLength = sizeof(kContentLength) - 1;
    while (!line.empty()) {
      UTIL_THROW_IF(!header.Line(line), util::EndOfFileException, "WARC ended in header.");
      if (line.size() >= kContentLengthLength && !strncasecmp(line.data(), kContentLength, kContentLengthLength)) {
        UTIL_THROW_IF(seen_content_length, WARCReadException, reader_ << "Two Content-Length headers?");
        seen_content_length = true;
        char *end;
        length = std::strtoll(line.data() + kContentLengthLength, &end, 10);
        // TODO: tolerate whitespace?
        UTIL_THROW_IF(end != line.data() + line.size(), WARCReadException, reader_ << "Content-Length parse error in `" << line << '\'');
      }
    }
    UTIL_THROW_IF(!seen_content_length, WARCReadException, reader_ << "No Content-Length: header in " << out.str);
    std::size_t total_length = header.Consumed() + length + 4 /* CRLF CRLF after data as specified in the standard. */;

    if (total_length < out.str.size()) {
      overhang_.assign(out.str.data() + total_length, out.str.size() - total_length);
      out.str.resize(total_length);
    } else if (total_length > size_limit) { // Skip records that are too long
      std::size_t start = out.str.size();
      while (start != total_length) { // Skip by decompressing the body, but not storing it.
        std::size_t got = reader_.Read(&out.str[0], out.str.size());
        UTIL_THROW_IF(!got, util::EndOfFileException, "Unexpected end of file while reading content of length " << out.str.size());
        start += got;
      }
      out.skipped = start;
      out.str.clear();
      return true; // Don't bother checking the trailing \r\n\r\n
    } else {
      std::size_t start = out.str.size();
      out.str.resize(total_length);
      while (start != out.str.size()) {
        std::size_t got = reader_.Read(&out.str[start], out.str.size() - start);
        UTIL_THROW_IF(!got, util::EndOfFileException, "Unexpected end of file while reading content of length " << length);
        start += got;
      }
    }
    // Check CRLF CRLF.
    UTIL_THROW_IF(util::StringPiece(out.str.data() + out.str.size() - 4, 4) != util::StringPiece("\r\n\r\n", 4), WARCReadException, reader_ << "End of WARC record missing CRLF CRLF");
    return true;
  } catch (util::CompressedException const &e) {
    std::cerr << "Caught CompressedException while reading record: " << e.what() << std::endl;
    // We got a decompression error at this position in the reader. Let's make it
    // jump forward to the next (possible) decodable chunk by searching for the
    // next bit of magic. And return an empty record just to make clear we
    // skipped a bit.
    out.str.clear();
    out.skipped = offsets_.empty() ? reader_.Skip() : reader_.SkipTo(offsets_);
    return true;
  } catch (WARCReadException const &e) {
    std::cerr << "Caught WARCReadException while reading record: " << e.what() << std::endl;
    out.str.clear();
    out.skipped = offsets_.empty() ? reader_.Skip() : reader_.SkipTo(offsets_);
    return true;
  }
}

} // namespace preprocess
