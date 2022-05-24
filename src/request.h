#include <iostream>
#include <vector>
#include <string>


class scan_req {
    private:
        std::vector<std::string> proj_cols;
        std::string filter_expr;

    public:

        scan_req(std::vector<string> proj_cols, std::string filter_expr)
        : proj_cols(proj_cols), filter_expr(filter_expr) {}

        template<typename A>
        void serialize(A& ar) {
            ar & proj_cols;
            ar & filter_expr;
        }
};