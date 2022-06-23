#include "payload.h"
#include "util.h"


namespace cp = arrow::compute;

class ScanResultConsumer {
    public:
        ScanResultConsumer(
            std::shared_ptr<arrow::RecordBatchReader> reader, std::shared_ptr<cp::ExecPlan> plan)
            : reader(reader), plan(plan) {}
        std::shared_ptr<arrow::RecordBatchReader> reader;
        std::shared_ptr<cp::ExecPlan> plan;
};

// arrow::Result<std::shared_ptr<ScanResultConsumer>> Scan(cp::ExecContext& exec_context, const ScanReqRPCStub& stub) {
//     ARROW_ASSIGN_OR_RAISE(auto filter,
//                             arrow::compute::Deserialize(std::make_shared<arrow::Buffer>(
//                             stub.filter_buffer, stub.filter_buffer_size)));

//     arrow::ipc::DictionaryMemo empty_memo;
//     arrow::io::BufferReader schema_reader(stub.projection_buffer, stub.projection_buffer_size);
//     ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ipc::ReadSchema(&schema_reader, &empty_memo));

    // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
    //                         cp::ExecPlan::Make(&exec_context));

//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

//     ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
//     ARROW_RETURN_NOT_OK(scanner_builder->Project(schema->field_names()));
//     ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));

//     ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

//     ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());

//     auto consumer = std::make_shared<ScanResultConsumer>(reader, plan);
//     return consumer;
// }


arrow::Result<std::shared_ptr<ScanResultConsumer>> ScanB(cp::ExecContext& exec_context, const ScanReqRPCStub& stub) {
    std::string uri = "file:///mnt/cephfs/dataset";
    
    std::string path;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &path)); 
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
      
    arrow::fs::FileSelector s;
    s.base_dir = std::move(path);
    s.recursive = true;

    auto filter = 
        arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                arrow::compute::literal(-200));

    arrow::dataset::FileSystemFactoryOptions options;
    ARROW_ASSIGN_OR_RAISE(auto factory, 
      arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
    arrow::dataset::FinishOptions finish_options;
    ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                          cp::ExecPlan::Make(&exec_context));

    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    ARROW_RETURN_NOT_OK(scanner_builder->Project({"passenger_count", "fare_amount"}));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());

    auto consumer = std::make_shared<ScanResultConsumer>(reader, plan);
    return consumer;
}
