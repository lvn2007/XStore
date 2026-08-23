#ifndef XSTORE_LOGGER_H
#define XSTORE_LOGGER_H

#include <filesystem>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/stopwatch.h>

class Logger {
    public:
        Logger(spdlog::level::level_enum console_level, spdlog::level::level_enum file_level, unsigned short int flush_interval = 5) {
            // Console outputs
            std::shared_ptr<spdlog::sinks::sink> console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            console_sink->set_level(console_level);

            // File outputs
            std::filesystem::create_directories("logs");
            std::shared_ptr<spdlog::sinks::sink> file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/server.log", true);
            file_sink->set_level(file_level);

            // Group multi-sinks into one entity
            std::shared_ptr<spdlog::logger> spd_logger = std::make_shared<spdlog::logger>(spdlog::logger("SERVER", {console_sink, file_sink}));
            spd_logger->set_level(spdlog::level::trace);
            spdlog::set_default_logger(spd_logger);
            spdlog::flush_every(std::chrono::seconds(flush_interval));
        }
};

#endif //XSTORE_LOGGER_H
