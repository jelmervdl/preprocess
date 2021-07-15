#include "util/file_piece.hh"
#include "util/file_stream.hh"
#include "util/fixed_array.hh"
#include "captive_child.hh"
#include <thread>
#include <unistd.h>
#include <memory> 

/**
 * Round-robin parallel, the simple cousin of Queue-based greedy parallel. This 
 * one just assigns a line to the workers sequentially. Because the order is
 * fixed, it can rely entirely on blocking IO for synchronisation. It also has
 * a much lower memory usage (which in theory could be even lower if we bother
 * to replace ReadLineOrEOF() with CopyLineToOStreamOrEOF()).
 * 
 * This one actually doesn't work so well: say you write to worker A, but not
 * enough for it to cause output. Then you start writing to worker B, but that
 * blocks because there is too much input. Deadlock! WriteOutput is still
 * blocking on worker A and thus never frees up the buffers of worker B that
 * would allow the ReadInput loop to come back to worker A to fill it up enough
 * to start producing output.
 * 
 * When it does work though (i.e. with most line-based programs) it is much
 * less resource intensive and often faster than qparallel.
 */

struct Options {
	std::size_t n_workers;
	char** child_argv;
};

struct Worker {
	util::scoped_fd in_file;
	util::scoped_fd out_file;
	pid_t child_;

	std::unique_ptr<util::FileStream> in;
	std::unique_ptr<util::FilePiece> out;

	Worker(char** argv)
	: child_(preprocess::Launch(argv, in_file, out_file))
	, in(new util::FileStream(in_file.release()))
	, out(nullptr)
	{
		//
	}

	int Join() {
		return preprocess::Wait(child_);
	}
};

void WriteOutput(util::FixedArray<Worker> &workers, int fileno) {
	util::FileStream out(fileno);

	std::size_t lineno = 0;
	std::size_t open = workers.size();
	util::StringPiece line;

	while (open > 0) {
		std::size_t i = lineno++ % workers.size();
		Worker &worker = workers[i];

		if (!worker.out)
			worker.out.reset(new util::FilePiece(worker.out_file.release()));
		
		if (worker.out->ReadLineOrEOF(line))
			out << line << '\n';
		else {
			--open;
		}
	}
}


int Usage(char** argv) {
	std::cerr << "Usage: " << argv[0] << " [-j 4] child [args..]\n"
	          << "Round-robin line 'scheduler'" << std::endl;
	return 1;
}


void ParseOptions(Options &options, int argc, char **argv) {
	while (true) {
		switch(getopt(argc, argv, "+j:h")) {
			case 'j':
				options.n_workers = std::atoi(optarg);
				continue;

			case 'h':
			case '?':
			default:
				std::exit(Usage(argv));

			case -1:
				break;
		}
		break;
	}

	if (optind == argc)
		std::exit(Usage(argv));

	options.child_argv = argv + optind;
}


int main(int argc, char** argv) {
	Options options;
	ParseOptions(options, argc, argv);

	// Create workers and worker input streams. Don't create output streams
	// for them yet as those contructors are blocking.
	util::FixedArray<Worker> workers(options.n_workers);
	for (int i = 0; i < options.n_workers; ++i)
		workers.emplace_back(options.child_argv);

	// Create worker output reader thread
	std::thread write_output(WriteOutput, std::ref(workers), STDOUT_FILENO);

	util::FilePiece in(STDIN_FILENO, NULL, &std::cerr);
	std::size_t lineno = 0;
	util::StringPiece line;

	// Give each worker a line: round robin style!
	while (in.ReadLineOrEOF(line))
		*(workers[lineno++ % workers.size()].in) << line << '\n';

	// Tell all workers that that's it for today: close their input streams.
	for (auto &worker : workers)
		worker.in.reset();

	// Wait for workers to exit
	int exit_code = 0;
	for (auto &worker : workers)
		exit_code = std::max(exit_code, worker.Join());

	// Wait for WriteOutput to finish
	write_output.join();

	return exit_code;
}