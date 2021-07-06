#include "captive_child.hh"
#include "util/pcqueue.hh"
#include "util/file_piece.hh"
#include "util/file_stream.hh"
#include "util/fixed_array.hh"
#include <future>
#include <vector>
#include <unistd.h>

struct Future {
	bool valid;
	std::future<std::string> future;

	Future()
	: valid(false)
	, future() {
		//
	}

	Future(std::future<std::string> &&future)
	: valid(true)
	, future(std::move(future)) {
		//
	}
};


struct Promise {
	bool valid;
	std::promise<std::string> promise;

	Promise()
	: valid(false)
	, promise() {
		//
	}

	Promise(std::promise<std::string> &&promise)
	: valid(true)
	, promise(std::move(promise)) {
		//
	}
};


struct Task {
	std::string input;
	std::promise<std::string> output;
};


void ReadInput(int from, util::PCQueue<Task> *tasks, util::UnboundedSingleQueue<Future> *futures) {
	util::FilePiece in(from);

	for (util::StringPiece line : in) {
		Promise promise;
		futures->Produce(std::move(promise.promise.get_future()));
		Task task{line.as_string(), std::move(promise.promise)};
		tasks->ProduceSwap(task);
	}
}


void WriteOutput(int to, util::UnboundedSingleQueue<Future> *futures) {
	util::FileStream out(to);
	Future future;

	while (futures->Consume(future).valid)
		out << future.future.get() << '\n';
}


void InputToProcess(util::PCQueue<Task> *tasks, util::scoped_fd process_in, util::UnboundedSingleQueue<Promise> *promises) {
	util::FileStream in(process_in.release());
	Task task;

	while (!tasks->ConsumeSwap(task).input.empty()) {
		in << task.input << '\n';
		promises->Produce(std::move(task.output));
	}

	promises->Produce(Promise()); // Tells OutputFromProcess to stop
}


void OutputFromProcess(util::scoped_fd process_fd, util::UnboundedSingleQueue<Promise> *promises) {
	util::FilePiece process_out(process_fd.release());

	Promise promise;
	while (promises->Consume(promise).valid)
		promise.promise.set_value(process_out.ReadLine().as_string());
}


class Worker {
	public:
		Worker(util::PCQueue<Task> &in, char *argv[]) {
			util::scoped_fd in_file, out_file;

			child_ = preprocess::Launch(argv, in_file, out_file);
			input_ = std::thread(InputToProcess, &in, std::move(in_file), &promises_);
			output_ = std::thread(OutputFromProcess, std::move(out_file), &promises_);
		}

		int Join() {
			input_.join();
			output_.join();
			return preprocess::Wait(child_);
		}

	private:
		pid_t child_;
		std::thread input_, output_;
		util::UnboundedSingleQueue<Promise> promises_;
};


struct Options {
	std::size_t n_workers = std::thread::hardware_concurrency();
	char **child_argv = nullptr;
};


int Usage(char** argv) {
	std::cerr << "Usage: " << argv[0] << " [-j 4] child [args..]" << std::endl;
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


int main(int argc, char* argv[]) {
	Options options;

	ParseOptions(options, argc, argv);

	util::PCQueue<Task> tasks(options.n_workers);
	util::UnboundedSingleQueue<Future> futures;

	util::FixedArray<Worker> workers(options.n_workers);

	for (int i = 0; i < options.n_workers; ++i)
		workers.emplace_back(tasks, options.child_argv);

	std::thread write(WriteOutput, STDOUT_FILENO, &futures);

	ReadInput(STDIN_FILENO, &tasks, &futures);

	for (int i = 0; i < options.n_workers; ++i) {
		Task task;
		tasks.ProduceSwap(task); // Empty task to tell InputToProcess to stop.
	}

	futures.Produce(Future()); // Empty future to tell WriteOutput to stop

	int exit_code = 0;

	// Wait for workers (and children) to exit
	for (Worker &worker : workers)
		exit_code = std::max(exit_code, worker.Join());

	// Wait for WriteOutput to finish
	write.join();

	return exit_code;
}