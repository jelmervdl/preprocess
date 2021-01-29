#pragma once

#include "util/compress.hh"

#include <string>
#include <vector>

namespace preprocess {

class WARCReadException : public util::Exception {
public:
  WARCReadException() throw();
  ~WARCReadException() throw();
};

class WARCReader {
  public:
    explicit WARCReader(int fd) : reader_(fd) {}

    explicit WARCReader(std::string const &file);

    typedef struct {
      std::size_t skipped;
      std::string str;
    } Record;
    
    bool Read(Record &out, std::size_t size_limit = -1);
    
  private:
    util::ReadCompressed reader_;

    std::string overhang_;

    std::vector<std::size_t> offsets_;
};

} // namespace preprocess
