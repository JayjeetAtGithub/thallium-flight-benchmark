#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    public:
        char *filter_buffer;
        size_t filter_buffer_size;

        scan_request() {}
        scan_request(char const* filter_buffer, size_t filter_buffer_size)
        : filter_buffer(filter_buffer), filter_buffer_size(filter_buffer_size) {}

        template<typename A>
        void save(A& ar) {
            ar & filter_buffer_size;
            ar.write(filter_buffer, filter_buffer_size);
        }

        template<typename A>
        void load(A& ar) {
            ar & filter_buffer_size;
            filter_buffer = new char[filter_buffer_size];
            ar.read(filter_buffer, filter_buffer_size);
        }
};
