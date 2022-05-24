#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    public:
        int val;

        scan_request(int val) : val(val) {}

        template<typename A>
        void serialize(A& ar) {
            ar(val);
        }
};