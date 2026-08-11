#ifndef XSTORE_LOGGER_H
#define XSTORE_LOGGER_H

#include <filesystem>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>

class Logger {
    public:
        Logger(spdlog::level::level_enum CONSOLE_LEVEL, spdlog::level::level_enum FILE_LEVEL, unsigned short int flushInterval = 5) {
            // Console outputs
            std::shared_ptr<spdlog::sinks::sink> ConsoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            ConsoleSink->set_level(CONSOLE_LEVEL);

            // File outputs
            std::filesystem::create_directories("logs");
            std::shared_ptr<spdlog::sinks::sink> FileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/server.log", true);
            FileSink->set_level(FILE_LEVEL);

            // Group multi-sinks into one entity
            std::shared_ptr<spdlog::logger> SpdLogger = std::make_shared<spdlog::logger>(spdlog::logger("SERVER", {ConsoleSink, FileSink}));
            SpdLogger->set_level(spdlog::level::trace);
            spdlog::set_default_logger(SpdLogger);
            spdlog::flush_every(std::chrono::seconds(flushInterval));
        }
};

#endif //XSTORE_LOGGER_H
