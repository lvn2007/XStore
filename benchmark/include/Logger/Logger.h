#ifndef XBENCH_LOGGER_H
#define XBENCH_LOGGER_H

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>

class Logger {
    public:
        Logger(spdlog::level::level_enum CONSOLE_LEVEL, spdlog::level::level_enum FILE_LEVEL, unsigned short int flushInterval = 5) {
            // Console outputs
            std::shared_ptr<spdlog::sinks::sink> consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            consoleSink->set_level(CONSOLE_LEVEL);

            // File outputs
            std::shared_ptr<spdlog::sinks::sink> fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/XBench.log", true);
            fileSink->set_level(FILE_LEVEL);

            // Group multi-sinks into one entity
            std::shared_ptr<spdlog::logger> logger = std::make_shared<spdlog::logger>(spdlog::logger("XBENCH", {consoleSink, fileSink}));
            logger->set_level(spdlog::level::trace);
            spdlog::set_default_logger(logger);
            spdlog::flush_every(std::chrono::seconds(flushInterval));
        }

    protected:
        std::shared_ptr<spdlog::logger> spdLogger;
};

#endif //XBENCH_LOGGER_H
