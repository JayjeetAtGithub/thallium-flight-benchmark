#include <iostream>
#include <vector>
#include <string>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <arrow/compute/exec/expression.h>


struct ConnCtx {
    thallium::engine engine;
    thallium::endpoint endpoint;
};

struct ScanCtx {
    std::string uuid;
    std::shared_ptr<arrow::Schema> schema;  
};

class ScanReqRPCStub {
    public:
        uint8_t *filter_buffer;
        size_t filter_buffer_size;

        uint8_t *dataset_schema_buffer;
        size_t dataset_schema_buffer_size;

        uint8_t *projection_buffer;
        size_t projection_buffer_size;

        ScanReqRPCStub() {}
        ScanReqRPCStub(
            uint8_t* filter_buffer, size_t filter_buffer_size,
            uint8_t* dataset_schema_buffer, size_t dataset_schema_buffer_size, 
            uint8_t* projection_buffer, size_t projection_buffer_size)
        : filter_buffer(filter_buffer), filter_buffer_size(filter_buffer_size),
          dataset_schema_buffer(dataset_schema_buffer),  dataset_schema_buffer_size(dataset_schema_buffer_size),
          projection_buffer(projection_buffer), projection_buffer_size(projection_buffer_size) {}

        template<typename A>
        void save(A& ar) const {
            ar & filter_buffer_size;
            ar.write(filter_buffer, filter_buffer_size);

            ar & dataset_schema_buffer_size;
            ar.write(dataset_schema_buffer, dataset_schema_buffer_size);

            ar & projection_buffer_size;
            ar.write(projection_buffer, projection_buffer_size);
        }

        template<typename A>
        void load(A& ar) {
            ar & filter_buffer_size;
            filter_buffer = new uint8_t[filter_buffer_size];
            ar.read(filter_buffer, filter_buffer_size);

            ar & dataset_schema_buffer_size;
            dataset_schema_buffer = new uint8_t[dataset_schema_buffer_size];
            ar.read(dataset_schema_buffer, dataset_schema_buffer_size);

            ar & projection_buffer_size;
            projection_buffer = new uint8_t[projection_buffer_size];
            ar.read(projection_buffer, projection_buffer_size);
        }
};

struct ScanReq {
    ScanReqRPCStub stub;
    arrow::compute::Expression filter;
    std::shared_ptr<arrow::Schema> schema;
};
