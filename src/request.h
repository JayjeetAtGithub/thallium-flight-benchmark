#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    public:
        int _val;

        scan_request(int val) : _val(val) {}
        template<typename A> friend void serialize(A& ar, scan_request& s);
};

template<typename A>
void serialize(A& ar, scan_request& sr) {
    ar(sr._val);
}
