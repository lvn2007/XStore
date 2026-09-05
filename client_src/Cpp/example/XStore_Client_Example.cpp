#include <iostream>

#include "XStore_Client.h"

// Minimal smoke test: construct the client (socket create + connect) and
// build one request message without requiring a live server. Actual RPCs
// (InfoRequest(), UnaryInsert(), ...) block on send/recv and need a running
// XStore server -- see client_src/Python/XStore.py for a fuller usage example.
int main() {
    XStoreClient client("127.0.0.1", "5555", /*timeoutMs=*/1000, /*warmupOnInit=*/false);

    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Double(3.14);

    std::cout << "XStore_Client constructed OK, sample RowData has "
              << row.fielddata_size() << " field(s)\n";
    return 0;
}
