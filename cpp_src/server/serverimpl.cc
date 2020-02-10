#include "server.h"

#include <vector>

#include "args/args.hpp"
#include "dbmanager.h"
#include "debug/allocdebug.h"
#include "debug/backtrace.h"
#include "httpserver.h"
#include "loggerwrapper.h"
#include "reindexer_version.h"
#include "rpcserver.h"
#include "serverimpl.h"
#include "statscollect/prometheus.h"
#include "statscollect/statscollector.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"
#include "yaml/yaml.h"

#ifdef _WIN32
#include "winservice.h"
#endif
#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
void init_resources() { CMRC_INIT(reindexer_server_resources); }
#else
void init_resources() {}
#endif

namespace reindexer_server {

using std::string;
using std::pair;
using std::vector;

using reindexer::fs::GetDirPath;
using reindexer::logLevelFromString;

ServerImpl::ServerImpl() : coreLogLevel_(LogNone), storageLoaded_(false), running_(false) { async_.set(loop_); }

Error ServerImpl::InitFromCLI(int argc, char *argv[]) {
	Error err = config_.ParseCmd(argc, argv);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromFile(const char *filePath) {
	Error err = config_.ParseFile(filePath);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromYAML(const string &yaml) {
	Error err = config_.ParseYaml(yaml);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::init() {
	using reindexer::fs::TryCreateDirectory;
	using reindexer::fs::ChownDir;
	using reindexer::fs::ChangeUser;
	Error err;

	init_resources();

	vector<string> dirs = {
#ifndef _WIN32
		GetDirPath(config_.DaemonPidFile),
#endif
		GetDirPath(config_.CoreLog),	   GetDirPath(config_.HttpLog), GetDirPath(config_.RpcLog),
		GetDirPath(config_.ServerLog),	   config_.StoragePath};

	for (const string &dir : dirs) {
		err = TryCreateDirectory(dir);
		if (!err.ok()) return err;
#ifndef _WIN32
		err = ChownDir(dir, config_.UserName);
		if (!err.ok()) return err;
#endif
	}

#ifndef _WIN32
	if (!config_.UserName.empty()) {
		err = ChangeUser(config_.UserName.c_str());
		if (!err.ok()) return err;
	}
	signal(SIGPIPE, SIG_IGN);
#endif

	coreLogLevel_ = logLevelFromString(config_.LogLevel);
	return 0;
}

int ServerImpl::Start() {
#ifndef _WIN32
	if (config_.Daemonize) {
		Error err = daemonize();
		if (!err.ok()) {
			std::cerr << err.what() << std::endl;
			return EXIT_FAILURE;
		}
	}
#else
	using reindexer::iequals;
	bool running = false;
	reindexer_server::WinService svc(
		"reindexer", "Reindexer server",
		[&]() {
			running = true;
			run();
			running = false;
		},
		[]() {	//
			raise(SIGTERM);
		},
		[&]() {	 //
			return running;
		});

	if (config_.InstallSvc) {
		auto &args = config_.Args();
		string cmdline = args.front();
		for (size_t i = 1; i < args.size(); i++) {
			cmdline += " ";
			if (!iequals(args[i], "--install")) {
				cmdline += args[i];
			} else {
				cmdline += "--service";
			}
		}
		return svc.Install(cmdline.c_str()) ? EXIT_SUCCESS : EXIT_FAILURE;
	} else if (config_.RemoveSvc) {
		return svc.Remove() ? EXIT_SUCCESS : EXIT_FAILURE;
	} else if (config_.SvcMode) {
		return svc.Start();
	}
#endif

	return run();
}

void ServerImpl::Stop() {
	if (running_) {
		running_ = false;
		async_.send();
	}
}

int ServerImpl::run() {
	loggerConfigure();
	reindexer::debug::backtrace_set_writer([](string_view out) {
		auto logger = spdlog::get("server");
		if (logger) {
			logger->info("{}", out);
			logger->flush();
		} else {
			std::cerr << std::endl << out;
		}
	});
	if (running_) {
		logger_.warn("attempting to start server, but already started.");
		return -1;
	}

	if (config_.DebugAllocs) {
#ifdef __APPLE__
		// tcmalloc + osx is crashing with thread_local storage access from malloc hooks
		allocdebug_init_mt();
#else
		allocdebug_init();
#endif

#if !REINDEX_WITH_GPERFTOOLS
		logger_.warn("debug.allocs is enabled in config, but reindexer complied without gperftools - Can't enable feature.");
#endif
	}

	if (config_.DebugPprof) {
#if REINDEX_WITH_GPERFTOOLS
		if (!std::getenv("HEAPPROFILE") && !std::getenv("TCMALLOC_SAMPLE_PARAMETER")) {
			logger_.warn(
				"debug.pprof is enabled, but TCMALLOC_SAMPLE_PARAMETER or HEAPPROFILE environment varables are not set. Heap profiling is "
				"not possible.");
		}
#elif REINDEX_WITH_JEMALLOC
		if (alloc_ext::JEMallocIsAvailable()) {
			size_t val = 0, sz = sizeof(size_t);
			alloc_ext::mallctl("config.prof", &val, &sz, NULL, 0);
			if (!val) {
				logger_.warn("debug.pprof is enabled, but jemalloc compiled without profiling support. Heap profiling is not possible.");
			} else {
				alloc_ext::mallctl("opt.prof", &val, &sz, NULL, 0);
				if (!val) {
					logger_.warn(
						"debug.pprof is enabled, but jemmalloc profiler is off. Heap profiling is not possible. export "
						"MALLOC_CONF=\"prof:true\" "
						"to enable it");
				}
			}
		} else {
			logger_.warn("debug.pprof is enabled in config, but reindexer can't link jemalloc library");
		}
#else
		logger_.warn("debug.pprof is enabled in config, but reindexer complied without gperftools or jemalloc - Can't enable feature.");
#endif
	}

	initCoreLogger();
	try {
		dbMgr_.reset(new DBManager(config_.StoragePath, !config_.EnableSecurity));

		auto status = dbMgr_->Init(config_.StorageEngine, config_.StartWithErrors, config_.Autorepair);
		if (!status.ok()) {
			logger_.error("Error init database manager: {0}", status.what());
			return EXIT_FAILURE;
		}
		storageLoaded_ = true;

		logger_.info("Starting reindexer_server ({0}) on {1} HTTP, {2} RPC, with db '{3}'", REINDEX_VERSION, config_.HTTPAddr,
					 config_.RPCAddr, config_.StoragePath);

#if LINK_RESOURCES
		if (config_.WebRoot.length() != 0) {
			logger_.warn("Reindexer server built with embeded web resources. Specified web root '{0}' will be ignored", config_.WebRoot);
			config_.WebRoot.clear();
		}
#endif
		LoggerWrapper httpLogger("http");

		std::unique_ptr<Prometheus> prometheus;
		std::unique_ptr<StatsCollector> statsCollector;
		if (config_.EnablePrometheus) {
			prometheus.reset(new Prometheus);
			statsCollector.reset(new StatsCollector(prometheus.get(), config_.PrometheusCollectPeriod));
		}

		HTTPServer httpServer(*dbMgr_, config_.WebRoot, httpLogger,
							  HTTPServer::OptionalConfig{config_.DebugAllocs, config_.DebugPprof, prometheus.get(), statsCollector.get()});
		if (!httpServer.Start(config_.HTTPAddr, loop_)) {
			logger_.error("Can't listen HTTP on '{0}'", config_.HTTPAddr);
			return EXIT_FAILURE;
		}

		LoggerWrapper rpcLogger("rpc");
		RPCServer rpcServer(*dbMgr_, rpcLogger, config_.DebugAllocs, statsCollector.get());
		if (!rpcServer.Start(config_.RPCAddr, loop_)) {
			logger_.error("Can't listen RPC on '{0}'", config_.RPCAddr);
			return EXIT_FAILURE;
		}
		auto sigCallback = [&](ev::sig &sig) {
			logger_.info("Signal received. Terminating...");
			running_ = false;
			sig.loop.break_loop();
		};

		if (statsCollector) statsCollector->Start(*dbMgr_);

		ev::sig sterm, sint, shup;

		if (enableHandleSignals_) {
			sterm.set(loop_);
			sterm.set(sigCallback);
			sterm.start(SIGTERM);
			sint.set(loop_);
			sint.set(sigCallback);
			sint.start(SIGINT);
#ifndef _WIN32
			auto sigHupCallback = [&](ev::sig &sig) {
				(void)sig;
				loggerReopen();
			};
			shup.set(loop_);
			shup.set(sigHupCallback);
			shup.start(SIGHUP);
#endif
		}

		async_.set([](ev::async &a) { a.loop.break_loop(); });
		async_.start();

		running_ = true;
		while (running_) {
			loop_.run();
		}
		logger_.info("Reindexer server terminating...");

		if (statsCollector) statsCollector->Stop();
		rpcServer.Stop();
		httpServer.Stop();
	} catch (const Error &err) {
		logger_.error("Unhandled exception occuried: {0}", err.what());
	}
	dbMgr_.reset();
	logger_.info("Reindexer server shutdown completed.");

	spdlog::drop_all();
	async_.reset();
	logger_ = LoggerWrapper();
	coreLogger_ = LoggerWrapper();
	return 0;
}

#ifndef _WIN32
Error ServerImpl::daemonize() {
	pid_t pid = ::fork();
	switch (pid) {
		// child process
		case 0:
			if (!pid_.Open(config_.DaemonPidFile.c_str())) return pid_.Status();
			umask(0);
			setsid();
			if (chdir("/")) {
				return Error(errLogic, "Could not change working directory. Reason: %s", strerror(errno));
			};

			close(STDIN_FILENO);
			close(STDOUT_FILENO);
			close(STDERR_FILENO);
			break;

		// fork error ...
		case -1:
			return Error(errLogic, "Could not fork process. Reason: %s", strerror(errno));

		// parent process
		default:
			exit(EXIT_SUCCESS);
			//	break;
	}
	return 0;
}
#endif

Error ServerImpl::loggerConfigure() {
	spdlog::drop_all();
	spdlog::set_async_mode(16384, spdlog::async_overflow_policy::discard_log_msg, nullptr, std::chrono::seconds(2));
	spdlog::set_level(spdlog::level::trace);
	spdlog::set_pattern("[%L%d/%m %T.%e %t] %v");

	vector<pair<string, string>> loggers = {
		{"server", config_.ServerLog}, {"core", config_.CoreLog}, {"http", config_.HttpLog}, {"rpc", config_.RpcLog}};

	for (auto &logger : loggers) {
		auto &fileName = logger.second;
		try {
			if (fileName == "stdout" || fileName == "-") {
				spdlog::stdout_color_mt(logger.first);
			} else if (!fileName.empty() && fileName != "none") {
				auto sink = sinks_.find(fileName);
				if (sink == sinks_.end()) {
					sink = sinks_.emplace(fileName, std::make_shared<spdlog::sinks::simple_file_sink_mt>(fileName)).first;
				}
				spdlog::create(logger.first, sink->second);
			}
		} catch (const spdlog::spdlog_ex &e) {
			return Error(errLogic, "Can't create logger for '%s' to file '%s': %s\n", logger.first, logger.second, e.what());
		}
	}
	coreLogger_ = "core";
	logger_ = "server";
	return 0;
}

void ServerImpl::initCoreLogger() {
	auto callback = [&](int level, char *buf) {
		if (level <= coreLogLevel_) {
			switch (level) {
				case LogNone:
					break;
				case LogError:
					coreLogger_.error(buf);
					break;
				case LogWarning:
					coreLogger_.warn(buf);
					break;
				case LogTrace:
					coreLogger_.trace(buf);
					break;
				case LogInfo:
					coreLogger_.info(buf);
					break;
				default:
					coreLogger_.debug(buf);
					break;
			}
		}
	};
	if (coreLogLevel_) reindexer::logInstallWriter(callback);
	(void)callback;
}

ServerImpl::~ServerImpl() {
	if (coreLogLevel_) reindexer::logInstallWriter(nullptr);
}

}  // namespace reindexer_server
