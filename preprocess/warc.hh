#pragma once

#include "util/compress.hh"

#include <string>

namespace preprocess {

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
};

} // namespace preprocess
