#include <iostream>
#include <vector>
#include <string>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


struct ConnCtx {
    thallium::engine engine;
    thallium::endpoint endpoint;
};


class ScanRequest {
    public:
        uint8_t *filter_buffer;
        size_t filter_buffer_size;

        uint8_t *projection_buffer;
        size_t projection_buffer_size;

        ScanRequest() {}
        ScanRequest(
            uint8_t* filter_buffer, size_t filter_buffer_size, uint8_t* projection_buffer, size_t projection_buffer_size)
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
            filter_buffer = new uint8_t[filter_buffer_size];
            ar.read(filter_buffer, filter_buffer_size);

            ar & projection_buffer_size;
            projection_buffer = new uint8_t[projection_buffer_size];
            ar.read(projection_buffer, projection_buffer_size);
        }
};
