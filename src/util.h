#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>     

#include <iostream>
#include <memory>
#include <utility>
#include <vector>


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

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

template <class TYPE>
arrow::Result<std::shared_ptr<arrow::Array>> GetBinaryArrayDataSample(
    const std::vector<std::string>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSampleRecordBatch(
    const arrow::ArrayVector array_vector, const arrow::FieldVector& field_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto struct_result,
                        arrow::StructArray::Make(array_vector, field_vector));
  return record_batch->FromStructArray(struct_result);
}

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  auto null_long = std::numeric_limits<int64_t>::quiet_NaN();
  ARROW_ASSIGN_OR_RAISE(auto int64_array,
                        GetArrayDataSample<arrow::Int64Type>(
                            {1, 2, null_long, 3, null_long, 4, 5, 6, 7, 8}));

  arrow::BooleanBuilder boolean_builder;
  std::shared_ptr<arrow::BooleanArray> bool_array;

  std::vector<uint8_t> bool_values = {false, true,  true,  false, true,
                                      false, false, false, false, true};
  std::vector<bool> is_valid = {false, true,  true, true, true,
                                true,  false, true, true, true};

  ARROW_RETURN_NOT_OK(boolean_builder.Reserve(10));

  ARROW_RETURN_NOT_OK(boolean_builder.AppendValues(bool_values, is_valid));

  ARROW_RETURN_NOT_OK(boolean_builder.Finish(&bool_array));

  auto record_batch =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("a", arrow::int64()),
                                              arrow::field("b", arrow::boolean())}),
                               10, {int64_array, bool_array});
  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches({record_batch}));
  return table;
}

arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> GetDataset() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());
  auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  return ds;
}

std::string generate_uuid() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return boost::uuids::to_string(uuid);
}
