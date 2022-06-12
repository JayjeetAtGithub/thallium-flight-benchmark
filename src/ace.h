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

arrow::Result<std::shared_ptr<ScanResultConsumer>> Scan(cp::ExecContext& exec_context) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                            cp::ExecPlan::Make(&exec_context));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());

    auto consumer = std::make_shared<ScanResultConsumer>(reader, plan);
    return consumer;
}