#pragma once

#include "util/compress.hh"

#include <string>

namespace preprocess {

class WARCReader {
  public:
    explicit WARCReader(int fd) : reader_(fd) {}

    bool Read(std::string &out, std::size_t size_limit = -1);

  private:
    util::ReadCompressed reader_;

    std::string overhang_;
};

} // namespace preprocess
