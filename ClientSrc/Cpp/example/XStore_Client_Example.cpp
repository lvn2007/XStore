#include <iostream>

#include "XStore_Client.h"

// Minimal smoke test: construct the client (socket create + connect) and
// build one request message without requiring a live server. Actual RPCs
// (InfoRequest(), UnaryInsert(), ...) block on send/recv and need a running
// XStore server -- see ClientSrc/Python/XStore.py for a fuller usage example.
int main() {
    XStore_Client Client("127.0.0.1", "5555", /*timeoutMs=*/1000);

    XStore_Proto::RowData Row;
    *Row.add_fielddata() = XStore_FData::Double(3.14);

    std::cout << "XStore_Client constructed OK, sample RowData has "
              << Row.fielddata_size() << " field(s)\n";
    return 0;
}
