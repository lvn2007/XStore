// A fake XStore server for the client tests: a ROUTER that parses each request and answers it with whatever bytes a test function returns.
// Replies can be held back until `hold` requests are waiting, which shows whether a client sent them without waiting for an answer.
#pragma once

#include <algorithm>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "XStore_Client.h"

namespace xstore_test_client {

class AnsweringServer {
 public:
    // answer(index, request) -> the serialized reply message (one message). At most `count` requests are answered; the thread ends when no
    // request has come for 400 ms and no answer is being held back (held answers are sent after 400 ms of silence).
    AnsweringServer(size_t count, std::function<std::string(size_t index, const XStore_Proto::Request &)> answer, size_t hold = 1)
        : sock_(ctx_, zmq::socket_type::router) {
        sock_.set(zmq::sockopt::rcvtimeo, 400);
        sock_.set(zmq::sockopt::linger, 2000);
        sock_.bind("tcp://127.0.0.1:*");
        const std::string ep = sock_.get(zmq::sockopt::last_endpoint);
        port_ = ep.substr(ep.rfind(':') + 1);
        thread_ = std::thread([this, count, hold, answer = std::move(answer)] {
            struct Pending {
                zmq::message_t identity;
                std::string reply;
            };
            std::vector<Pending> pending;
            auto flush = [&] {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    max_pending_ = std::max(max_pending_, pending.size());
                }
                for (Pending &p : pending) {
                    (void)sock_.send(p.identity, zmq::send_flags::sndmore);
                    (void)sock_.send(zmq::message_t(), zmq::send_flags::sndmore);
                    (void)sock_.send(zmq::buffer(p.reply), zmq::send_flags::none);
                }
                pending.clear();
            };
            for (size_t i = 0; i < count;) {
                std::vector<zmq::message_t> request;   // [identity][empty][body]
                if (!zmq::recv_multipart(sock_, std::back_inserter(request))) {
                    if (pending.empty()) break;   // quiet and nothing owed: done
                    flush();                      // quiet with answers held back: the client is waiting for them, answer and go on
                    continue;
                }
                if (request.size() != 3) break;
                ++i;
                XStore_Proto::Request parsed;
                if (!parsed.ParseFromArray(request[2].data(), static_cast<int>(request[2].size()))) break;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    requests_.push_back(parsed);
                    sizes_.push_back(request[2].size());
                }
                pending.push_back({std::move(request[0]), answer(i - 1, parsed)});
                if (pending.size() >= hold) flush();
            }
            flush();   // nothing more is coming: answer what is waiting
        });
    }
    ~AnsweringServer() { thread_.join(); }
    const std::string &port() const { return port_; }
    std::vector<XStore_Proto::Request> requests() {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_;
    }
    std::vector<size_t> sizes() {
        std::lock_guard<std::mutex> lock(mutex_);
        return sizes_;
    }
    // The most requests that were waiting for their answer at once.
    size_t max_pending() {
        std::lock_guard<std::mutex> lock(mutex_);
        return max_pending_;
    }

 private:
    zmq::context_t ctx_;
    zmq::socket_t sock_;
    std::string port_;
    std::thread thread_;
    std::mutex mutex_;
    std::vector<XStore_Proto::Request> requests_;
    std::vector<size_t> sizes_;
    size_t max_pending_ = 0;
};

}  // namespace xstore_test_client
