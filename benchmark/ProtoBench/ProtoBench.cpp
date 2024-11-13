#include "ProtoBench.h"

std::vector<std::vector<double>> ProtoBench::run(unsigned int N_ENTRIES, unsigned int BYTE_SIZE, unsigned int N_ITERATIONS) {
    XStore_Proto::batchQuery_REP batchQueryRep;
    XStore_Proto::unaryQuery_REP *currEntry;
    XStore_Proto::rowData rowData;
    XStore_Proto::fData *field;
    std::string serializedData;
    XStore_Proto::batchQuery_REP deserializeBatchQueryRep;

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> serializeStart, serializeEnd, deSerializeStart, deSerializeEnd;

    // Serialize & Deserialize costs
    std::vector<double> serializeCost;
    std::vector<double> deSerializeCost;

    for (int i = 0; i < N_ITERATIONS; i++) {
        currEntry = batchQueryRep.add_queryresponse();
        currEntry->set_timestamp(randInt(1, 999'999'999));

        // Loop through each "column" in a row
        for (int j = 0; j < N_ENTRIES; j++) {
            field = rowData.add_fielddata();
            field->set_stringdata(randString(BYTE_SIZE));
        }
        currEntry->mutable_data()->MergeFrom(rowData);
        field->Clear();
        rowData.Clear();

        if (i == 0) {
            spdlog::info("[PROTO BENCH] Actual Size: {}B", batchQueryRep.ByteSizeLong());
        }

        // Serialize
        clearCaches();
        serializeStart = std::chrono::high_resolution_clock::now();
        serializedData = batchQueryRep.SerializeAsString();
        serializeEnd = std::chrono::high_resolution_clock::now();
        serializeCost.push_back(std::chrono::duration<double, std::milli> (serializeEnd - serializeStart).count());

        // Clear main buffer
        batchQueryRep.Clear();

        // Deserialize
        clearCaches();
        deSerializeStart = std::chrono::high_resolution_clock::now();
        deserializeBatchQueryRep.ParseFromArray(serializedData.data(), serializedData.size());
        deSerializeEnd = std::chrono::high_resolution_clock::now();
        deSerializeCost.push_back(std::chrono::duration<double, std::milli> (deSerializeEnd - deSerializeStart).count());
    }
    return std::vector{ serializeCost, deSerializeCost };
}