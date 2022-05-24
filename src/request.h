#include <iostream>
#include <vector>
#include <string>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>


class scan_request {
    private:
        std::vector<std::string> proj_cols;
        std::string filter_expr;

    public:
        scan_request(std::vector<std::string> proj_cols, std::string filter_expr)
        : proj_cols(proj_cols), filter_expr(filter_expr) {}

        template<typename A>
        void serialize(A& ar) {
            ar(proj_cols, filter_expr);
        }
};