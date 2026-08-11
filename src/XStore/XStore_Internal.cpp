#include "include/XStore/XStore_Internal.h"

std::string* SetTaggedStatus(std::string *status, const char *tag, const std::string &body) {
    status->reserve(std::strlen(tag) + body.size());
    *status = tag;
    status->append(body);
    return status;
}
