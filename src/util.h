#include <arrow/api.h>

std::shared_ptr<arrow::DataType> type_from_id(int type_id) {
    std::shared_ptr<arrow::DataType> type;
    switch(type_id) {
        case 9:
            type = arrow::int64();
            break;
        case 12:
            type = arrow::float64();
            break;
        case 13:
            type = arrow::binary();
            break;
        default:
            std::cout << "Unknown type" << std::endl;
            exit(1);
    }
    return type;     
}