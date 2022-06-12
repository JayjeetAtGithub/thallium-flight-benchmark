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
    // instantiate an exec plan
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                            cp::ExecPlan::Make(&exec_context));

    // get a dummy dataset
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

    // create an empty projection
    auto options = std::make_shared<arrow::dataset::ScanOptions>();

    // construct the scan node
    cp::ExecNode* scan;
    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
    ARROW_ASSIGN_OR_RAISE(scan,
                            cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
    ARROW_RETURN_NOT_OK(
        cp::MakeExecNode("sink", plan.get(), {scan}, cp::SinkNodeOptions{&sink_gen}));

    std::shared_ptr<arrow::RecordBatchReader> sink_reader =
        cp::MakeGeneratorReader(dataset->schema(), std::move(sink_gen), exec_context.memory_pool());

    auto consumer = std::make_shared<ScanResultConsumer>(sink_reader, plan);
    return consumer;
}