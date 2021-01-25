#include "captive_child.hh"
#include "warc.hh"

#include "util/compress.hh"
#include "util/file_stream.hh"
#include "util/file.hh"
#include "util/fixed_array.hh"
#include "util/pcqueue.hh"

#include <sys/types.h>
#include <sys/wait.h>

#include <mutex>
#include <string>
#include <thread>
#include <iomanip>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

namespace preprocess {
namespace {

class NameTemplate {
  public:
    NameTemplate(std::string const &tpl) {
      //Find the last X
      std::size_t end = tpl.find_last_of('X');
      std::size_t start = end;

      UTIL_THROW_IF(end == std::string::npos, util::Exception, "There are no X-es in the template name.");

      // Move cur to the first X of that sequence
      while (start > 0 && tpl[start-1] == 'X')
        --start;

      prefix_ = tpl.substr(0, start);
      suffix_ = tpl.substr(end + 1);
      padding_ = 1 + end - start;
    }

    std::string Format(std::size_t n) const {
      std::ostringstream formatted;
      formatted << prefix_ << std::setfill('0') << std::setw(padding_) << n << suffix_;
      return formatted.str();
    }
  private:
    std::string prefix_;
    std::string suffix_;
    std::size_t padding_;
};

// Wrapper around FileStream that writes at most bytes_limit bytes per file
// before moving to the next. Shares the "same" write(void*,size_t) interface.
// The file template name is in the format of nameXXX where all X-es are
// replaced by the file number with padding zeros. The X-es have to be at the
// end of the name.
class SplitFileStream {
  public:
    explicit SplitFileStream(const std::string &tpl, std::size_t bytes_limit, std::size_t buffer_size = 8192)
    : tpl_(tpl),
      bytes_limit_(bytes_limit),
      file_n_(0),
      file_fd_(),
      bytes_written_(0),
      file_stream_(-1, buffer_size) {
        
    }

    SplitFileStream(SplitFileStream &&from) noexcept
    : tpl_(std::move(from.tpl_)),
      bytes_limit_(from.bytes_limit_),
      file_n_(from.file_n_),
      file_fd_(std::move(from.file_fd_)),
      bytes_written_(from.bytes_written_),
      file_stream_(std::move(from.file_stream_)) {
        //
    }

    // Lower case "write" so it has the same interface as util::FileStream.
    SplitFileStream &write(const void *data, std::size_t length) {
      // If first write, or write that pushes us over the limit
      if (file_fd_.get() == -1 || bytes_written_ + length > bytes_limit_)
        OpenNext();

      file_stream_.write(data, length);
      bytes_written_ += length;

      return *this;
    }
  private:
    void OpenNext() {
      std::string filename(tpl_.Format(file_n_++));
      int fd = util::CreateOrThrow(filename.c_str());
      file_stream_.SetFD(fd);

      file_fd_.reset(fd); // Closes old file, hence call after SetFD which might flush.
      bytes_written_ = 0;
    }

    NameTemplate tpl_;
    std::size_t bytes_limit_;
    std::size_t file_n_;
    util::scoped_fd file_fd_;
    std::size_t bytes_written_;

    util::FileStream file_stream_; // Should be destroyed before file_fd_
};

// Thread to read from queue and dump to a worker.  Steals process_in.
void InputToProcess(util::PCQueue<std::string> *queue, int process_in) {
  // Steal fd for consistency with OutputFromProcess.
  util::scoped_fd fd(process_in);
  std::string warc;
  while (true) {
    queue->ConsumeSwap(warc);
    if (warc.empty()) return;
    util::WriteOrThrow(process_in, warc.data(), warc.size());
  }
}

// Thread to write from a worker to output.  Steals process_out.
template <class OutStream>  void OutputFromProcess(bool compress, int process_out, OutStream *out, std::mutex *out_mutex) {
  WARCReader reader(process_out);
  std::string str;
  if (compress) {
    std::string compressed;
    while (reader.Read(str)) {
      util::GZCompress(str, compressed);
      std::lock_guard<std::mutex> guard(*out_mutex);
      out->write(compressed.data(), compressed.size());
    }
  } else {
    while (reader.Read(str)) {
      std::lock_guard<std::mutex> guard(*out_mutex);
      out->write(str.data(), str.size());
    }
  }
}

// Thread to read WARC input from a file.  Steals from.  Does not poison the queue.
void ReadInput(int from, util::PCQueue<std::string> *queue) {
  preprocess::WARCReader reader(from);
  std::string str;
  while (reader.Read(str, 20 * 1024 * 1024)) { // 20M, same limit as warc2text
    if (!str.empty()) // Skipped records show up as empty records
      queue->ProduceSwap(str);
  }
}

// A child process going from WARC to WARC.
template <class OutStream> class Worker {
  public:
    Worker(util::PCQueue<std::string> &in, OutStream &out, std::mutex &out_mutex, bool compress, char *argv[]) {
      util::scoped_fd in_file, out_file;
      Launch(argv, in_file, out_file);
      input_ = std::thread(InputToProcess, &in, in_file.release());
      output_ = std::thread(OutputFromProcess<OutStream>, compress, out_file.release(), &out, &out_mutex);
    }

    void Join() {
      input_.join();
      output_.join();
    }

  private:
    std::thread input_, output_;
};

void ChildReaper(std::size_t expect) {
  try {
    for (; expect; --expect) {
      int wstatus;
      pid_t process = waitpid(-1, &wstatus, 0);
      UTIL_THROW_IF(-1 == process, util::ErrnoException, "waitpid");
      UTIL_THROW_IF(!WIFEXITED(wstatus), util::Exception, "Child process " << process << " terminated abnormally.");
      UTIL_THROW_IF(WEXITSTATUS(wstatus), util::Exception, "Child process " << process << " terminated with code " << WEXITSTATUS(wstatus) << ".");
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    abort();
  }
}

template <class OutStream> class WorkerPool {
  public:
    WorkerPool(std::size_t number, OutStream &out, bool compress, char *argv[]) : in_(number), workers_(number) {
      for (std::size_t i = 0; i < number; ++i) {
        workers_.push_back(in_, out, out_mutex_, compress, argv);
      }
      child_reaper_ = std::thread(ChildReaper, number);
    }

    util::PCQueue<std::string> &InputQueue() { return in_; }

    void Join() {
      // Poison all of them first.
      std::string str;
      for (std::size_t i = 0; i < workers_.size(); ++i) {
        in_.Produce(str);
      }
      for (Worker<OutStream> &i : workers_) {
        i.Join();
      }
      child_reaper_.join();
    }
    
  private:
    util::PCQueue<std::string> in_;
    std::mutex out_mutex_;
    util::FixedArray<Worker<OutStream>> workers_;

    std::thread child_reaper_;
};

struct Options {
  std::vector<std::string> inputs;
  std::size_t workers;
  std::string output;
  std::size_t bytes;
  bool compress;
};

void ParseBoostArgs(int restricted_argc, int real_argc, char *argv[], Options &out) {
  namespace po = boost::program_options;
  po::options_description desc("Arguments");
  desc.add_options()
    ("help,h", po::bool_switch(), "Show this help message")
    ("inputs,i", po::value(&out.inputs)->multitoken(), "Input files, which will be read in parallel and jumbled together.  Default: read from stdin.")
    ("output,o", po::value(&out.output), "Output filename or prefix if --bytes is used.")
    ("jobs,j", po::value(&out.workers)->default_value(std::thread::hardware_concurrency()), "Number of child process workers to use.")
    ("gzip,z", po::bool_switch(&out.compress), "Compress output in gzip format")
    ("bytes,b", po::value(&out.bytes)->default_value(1024 * 1024 * 1024), "Maximum filesize per output chunk.");
  po::variables_map vm;
  po::store(po::command_line_parser(restricted_argc, argv).options(desc).run(), vm);
  if (real_argc == 1 || vm["help"].as<bool>()) {
    std::cerr <<
      "Parallelizes WARC to WARC processing by wrapping a child process.\n"
      "Example that just does cat: " << argv[0] << " cat\n"
      "Arguments can be specified to control threads and files. Use -- to\n"
      "distinguish between file names and the command to wrap.\n" <<
      desc <<
      "Examples:\n" <<
      argv[0] << " -j 20 ./process_warc.sh\n" <<
      argv[0] << " -i a.warc b.warc -- ./process_warc.sh\n"
      "process_warc.sh is expected to take WARC on stdin and produce WARC on stdout.\n";
    exit(1);
  }
  po::notify(vm);
}

// Figuring out where the command line for the child is.
char **FindChild(int argc, char *argv[]) {
  // Pass help over to boost.
  if (argc == 1) return argv + 1;
  bool used_inputs = false;
  for (int i = 1; i < argc;) {
    char *a = argv[i];
    if (!strcmp(a, "--help") || !strcmp(a, "-h")) {
      // Help, doesn't matter, just make sure command is past that.
      return argv + i + 1;
    } else if (!strcmp(a, "--gzip") || !strcmp(a, "-z")) {
      i += 1;
    } else if (!strcmp(a, "--jobs") || !strcmp(a, "-j")) {
      UTIL_THROW_IF2(i + 1 == argc, "Expected argument to jobs");
      i += 2;
    } else if (!strcmp(a, "--bytes") || !strcmp(a, "-b")) {
      UTIL_THROW_IF2(i + 1 == argc, "Expected argument to bytes");
      i += 2;
    } else if (!strcmp(a, "--output") || !strcmp(a, "-o")) {
      UTIL_THROW_IF2(i + 1 == argc, "Expected argument to output");
      i += 2;
    } else if (!strcmp(a, "--inputs") || !strcmp(a, "-i")) {
      used_inputs = true;
      // Consume everything that doesn't begin with -.
      for (++i; i < argc && argv[i][0] != '-'; ++i) {}
    } else if (!strcmp(a, "--")) {
      return argv + i + 1;
    } else {
      // Presumably a command line program?
      UTIL_THROW_IF2(a[0] == '-', "Unrecognized option " << a);
      return argv + i;
    }
  }
  std::cerr << "Did not find a child process to run on the command line.\n";
  if (used_inputs) {
    std::cerr << "When using --inputs, remember to terminate with --.\n";
  }
  exit(1);
}

template <class OutStream> void Run(OutStream &out, const Options &options, char *child[]) {
  WorkerPool<OutStream> pool(options.workers, out, options.compress, child);

  util::FixedArray<std::thread> readers(options.inputs.empty() ? 1 : options.inputs.size());
  if (options.inputs.empty()) {
    readers.push_back(ReadInput, 0, &pool.InputQueue());
  } else {
    for (const std::string &name : options.inputs) {
      readers.push_back(ReadInput, util::OpenReadOrThrow(name.c_str()), &pool.InputQueue());
    }
  }
  for (std::thread &r : readers) {
    r.join();
  }
  pool.Join();
}

} // namespace
} // namespace preprocess

int main(int argc, char *argv[]) {
  char **child = preprocess::FindChild(argc, argv);
  preprocess::Options options;
  preprocess::ParseBoostArgs(child - argv, argc, argv, options);

  if (!options.output.empty()) {
    preprocess::SplitFileStream out(options.output, options.bytes);
    Run(out, options, child);
  } else {
    util::FileStream out(1);
    Run(out, options, child);
  }
}
