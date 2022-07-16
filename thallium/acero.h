#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/engine/api.h>

namespace eng = arrow::engine;
namespace cp = arrow::compute;

class AceroBatchConsumer : public cp::SinkNodeConsumer {
 public:
  explicit AceroBatchConsumer(size_t tag) : tag_{tag} {}

  arrow::Status Init(const std::shared_ptr<arrow::Schema>& schema,
                     cp::BackpressureControl* backpressure_control) override {
    return arrow::Status::OK();
  }

  arrow::Status Consume(cp::ExecBatch batch) override {
    // Consume a batch of data
    // (just print its row count to stdout)
    std::cout << "-" << tag_ << " consumed " << batch.length << " rows" << std::endl;
    return arrow::Status::OK();
  }

  arrow::Future<> Finish() override {
    return arrow::Future<>::MakeFinished();
  }

 private:
  // A unique label for instances to help distinguish logging output if a plan has
  // multiple sinks
  //
  // In this example, this is set to the zero-based index of the relation tree in the plan
  size_t tag_;
};

void Scan() {
    std::shared_ptr<arrow::Buffer> serialized_plan;
    
}