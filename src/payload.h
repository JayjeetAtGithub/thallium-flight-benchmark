#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    public:
        char *filter_buffer;
        size_t filter_buffer_size;

        char *projection_buffer;
        size_t projection_buffer_size;

        scan_request() {}
        scan_request(
            char* filter_buffer, size_t filter_buffer_size, char* projection_buffer, size_t projection_buffer_size)
        : filter_buffer(filter_buffer), filter_buffer_size(filter_buffer_size), 
          projection_buffer(projection_buffer), projection_buffer_size(projection_buffer_size) {}

        template<typename A>
        void save(A& ar) const {
            ar & filter_buffer_size;
            ar.write(filter_buffer, filter_buffer_size);

            ar & projection_buffer_size;
            ar.write(projection_buffer, projection_buffer_size);
        }

        template<typename A>
        void load(A& ar) {
            ar & filter_buffer_size;
            filter_buffer = new char[filter_buffer_size];
            ar.read(filter_buffer, filter_buffer_size);

            ar & projection_buffer_size;
            projection_buffer = new char[projection_buffer_size];
            ar.read(projection_buffer, projection_buffer_size);
        }
};

class rdma_request {
    public:
        int64_t num_rows;
        int64_t num_cols;
        std::vector<int> types;
        std::vector<int64_t> data_buff_sizes;
        std::vector<int64_t> offset_buff_sizes;

        rdma_request() {}

        rdma_request(int64_t num_cols) : num_cols(num_cols) { 
            types.resize(num_cols);
            data_buff_sizes.resize(num_cols);
            offset_buff_sizes.resize(num_cols);
        }

        template<typename A>
        void save(A& ar) const {
            ar & num_rows;
            ar & num_cols;
            ar & types;
            ar & data_buff_sizes;
            ar & offset_buff_sizes;
        }

        template<typename A>
        void load(A& ar) {
            ar & num_rows;
            ar & num_cols;
            ar & types;
            ar & data_buff_sizes;
            ar & offset_buff_sizes;
        }
};
