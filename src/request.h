#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    private:
        int _val;
    public:
        scan_request(int val) : _val(val) {}
        int get_val() const { return _val; }
        template<typename A> friend void serialize(A& ar, scan_request& s);
};

template<typename A>
void serialize(A& ar, scan_request& sr) {
    ar & sr._val;
}
