#pragma once
// The streamed replies in progress, by the routing id of their client, so that the broker can cancel them when their client is gone.
//
// The client socket is a ROUTER with ZMQ_ROUTER_MANDATORY: a message for a client whose connection is closed is refused (EHOSTUNREACH) where it would
// be dropped without a word. The broker sees that when it relays a reply message, calls Mark(id), and every reply registered under that id is
// cancelled (ReplyWindow::Cancel). The producer of a reply asks ReplyWindow::Cancelled() where it asks whether to park, stops scanning, and a worker
// that resumes it only ends it. Without this the producer scanned and packed the rest of the reply for nobody.
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "include/Networking/ZMQ_StreamFlow.h"

namespace xstore::streamflow {

class GoneClients {
public:
    // A reply starts: once per streamed reply (not per chunk). Holds the window weakly: a reply that ends needs no unregistering.
    void Register(std::string_view client_id, const std::shared_ptr<ReplyWindow> &window) {
        std::lock_guard<std::mutex> lock(mutex_);
        by_client_[std::string(client_id)].push_back(window);
        // Finished replies and clients that came and went are swept every so often, so the map stays about as big as the live replies.
        if (++registrations_ % kSweepEvery == 0) Sweep();
    }

    // The client is gone: cancels its replies in progress and forgets it (a connection that comes back with the same id starts clean). Returns how
    // many replies were cancelled.
    size_t Mark(std::string_view client_id) {
        std::vector<std::weak_ptr<ReplyWindow>> windows;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = by_client_.find(std::string(client_id));
            if (it == by_client_.end()) return 0;
            windows = std::move(it->second);
            by_client_.erase(it);
        }
        size_t cancelled = 0;
        for (const std::weak_ptr<ReplyWindow> &weak : windows) {
            if (const std::shared_ptr<ReplyWindow> window = weak.lock()) {
                window->Cancel();
                ++cancelled;
            }
        }
        return cancelled;
    }

    size_t EntryCountForTests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return by_client_.size();
    }

private:
    static constexpr size_t kSweepEvery = 256;

    void Sweep() {
        for (auto it = by_client_.begin(); it != by_client_.end();) {
            auto &windows = it->second;
            windows.erase(std::remove_if(windows.begin(), windows.end(), [](const std::weak_ptr<ReplyWindow> &w) { return w.expired(); }), windows.end());
            it = windows.empty() ? by_client_.erase(it) : std::next(it);
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<std::weak_ptr<ReplyWindow>>> by_client_;
    size_t registrations_ = 0;
};

}  // namespace xstore::streamflow
