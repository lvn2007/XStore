#ifndef XSTORE_VALUESTRUCT_H
#define XSTORE_VALUESTRUCT_H

struct ValueStruct {
    unsigned int index;
    long double value;
    bool is_initialized = false;
};

#endif //XSTORE_VALUESTRUCT_H
