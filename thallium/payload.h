#include <iostream>
#include <vector>
#include <string>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <arrow/compute/expression.h>

namespace tl = thallium;


struct ConnCtx {
    thallium::engine engine;
    thallium::endpoint endpoint;
};

struct ScanCtx {
    std::string uuid;
    std::shared_ptr<arrow::Schema> schema;  
};

class ScanRespStub {
    public:
        std::vector<int32_t> data_offsets;
        std::vector<int32_t> data_sizes;
        std::vector<int32_t> off_offsets;
        std::vector<int32_t> off_sizes;
        std::vector<int32_t> batch_sizes;
        int32_t total_size;
        tl::bulk bulk;

        ScanRespStub() {}
        ScanRespStub(std::vector<int32_t> data_offsets, std::vector<int32_t> data_sizes, std::vector<int32_t> off_offsets, std::vector<int32_t> off_sizes, std::vector<int32_t> batch_sizes, int32_t total_size, tl::bulk bulk):
            data_offsets(data_offsets), data_sizes(data_sizes), off_offsets(off_offsets), off_sizes(off_sizes), batch_sizes(batch_sizes), total_size(total_size), bulk(bulk) {}

        template<class A>
        void serialize(A& ar) {
            ar & data_offsets;
            ar & data_sizes;
            ar & off_offsets;
            ar & off_sizes;
            ar & batch_sizes;
            ar & total_size;
            ar & bulk;
        }
};

class ScanRespStubPush {
    public:
        std::vector<int32_t> data_offsets;
        std::vector<int32_t> data_sizes;
        std::vector<int32_t> off_offsets;
        std::vector<int32_t> off_sizes;
        std::vector<int32_t> batch_sizes;
        int32_t total_size;

        ScanRespStubPush() {}
        ScanRespStubPush(std::vector<int32_t> data_offsets, std::vector<int32_t> data_sizes, std::vector<int32_t> off_offsets, std::vector<int32_t> off_sizes, std::vector<int32_t> batch_sizes, int32_t total_size):
            data_offsets(data_offsets), data_sizes(data_sizes), off_offsets(off_offsets), off_sizes(off_sizes), batch_sizes(batch_sizes), total_size(total_size) {}

        template<class A>
        void serialize(A& ar) {
            ar & data_offsets;
            ar & data_sizes;
            ar & off_offsets;
            ar & off_sizes;
            ar & batch_sizes;
            ar & total_size;
        }
};

class ScanReqRPCStub {
    public:
        uint8_t *filter_buffer;
        size_t filter_buffer_size;

        uint8_t *dataset_schema_buffer;
        size_t dataset_schema_buffer_size;

        uint8_t *projection_schema_buffer;
        size_t projection_schema_buffer_size;

        std::string path;

        ScanReqRPCStub() {}
        ScanReqRPCStub(
            std::string path,
            uint8_t* filter_buffer, size_t filter_buffer_size,
            uint8_t* dataset_schema_buffer, size_t dataset_schema_buffer_size, 
            uint8_t* projection_schema_buffer, size_t projection_schema_buffer_size)
        : path(path),
          filter_buffer(filter_buffer), filter_buffer_size(filter_buffer_size),
          dataset_schema_buffer(dataset_schema_buffer),  dataset_schema_buffer_size(dataset_schema_buffer_size),
          projection_schema_buffer(projection_schema_buffer), projection_schema_buffer_size(projection_schema_buffer_size) {}

        template<typename A>
        void save(A& ar) const {
            ar & path;

            ar & filter_buffer_size;
            ar.write(filter_buffer, filter_buffer_size);

            ar & dataset_schema_buffer_size;
            ar.write(dataset_schema_buffer, dataset_schema_buffer_size);

            ar & projection_schema_buffer_size;
            ar.write(projection_schema_buffer, projection_schema_buffer_size);
        }

        template<typename A>
        void load(A& ar) {
            ar & path;

            ar & filter_buffer_size;
            filter_buffer = new uint8_t[filter_buffer_size];
            ar.read(filter_buffer, filter_buffer_size);

            ar & dataset_schema_buffer_size;
            dataset_schema_buffer = new uint8_t[dataset_schema_buffer_size];
            ar.read(dataset_schema_buffer, dataset_schema_buffer_size);

            ar & projection_schema_buffer_size;
            projection_schema_buffer = new uint8_t[projection_schema_buffer_size];
            ar.read(projection_schema_buffer, projection_schema_buffer_size);
        }
};

struct ScanReq {
    ScanReqRPCStub stub;
    std::shared_ptr<arrow::Schema> schema;
};
