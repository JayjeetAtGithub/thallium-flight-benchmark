#include <iostream>
#include <thread>
#include <chrono>
#include <arrow/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/testing/random.h"
#include "arrow/util/key_value_metadata.h"

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <thallium.hpp>

#include "util.h"
#include "payload.h"


namespace tl = thallium;


arrow::Result<ScanRequest> GetScanRequest(cp::Expression filter, std::shared_ptr<arrow::Schema> schema) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> filter_buff, arrow::compute::Serialize(filter));
    ARROW_ASSIGN_OR_RAISE(auto projection_buff, arrow::ipc::SerializeSchema(*schema));
    ScanRequest request(
        const_cast<uint8_t*>(filter_buff->data()), filter_buff->size(), 
        const_cast<uint8_t*>(projection_buff->data()), projection_buff->size()
    );
    return request;
}

ConnCtx Init(std::string host) {
    ConnCtx ctx;
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(host);
    ctx.engine = engine;
    ctx.endpoint = endpoint;
    return ctx;
}

std::string Scan(ConnCtx &ctx, ScanRequest &req) {
    tl::remote_procedure scan = ctx.engine.define("scan");
    return scan.on(ctx.endpoint)(req);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch(ConnCtx &ctx, std::string uuid) {
    auto schema = arrow::schema({arrow::field("a", arrow::int64()),
                                 arrow::field("b", arrow::boolean())});
    std::shared_ptr<arrow::RecordBatch> batch;
    std::function<void(const tl::request&, int64_t&, int64_t&, std::vector<int>&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> f =
        [&ctx, &schema, &batch](const tl::request& req, int64_t& num_rows, int64_t& num_cols, std::vector<int>& types, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk& b) {
            std::vector<std::shared_ptr<arrow::Array>> columns;
            std::vector<std::unique_ptr<arrow::Buffer>> data_buffs(num_cols);
            std::vector<std::unique_ptr<arrow::Buffer>> offset_buffs(num_cols);
            std::vector<std::pair<void*,std::size_t>> segments(num_cols*2);
            
            for (int64_t i = 0; i < num_cols; i++) {
                data_buffs[i] = arrow::AllocateBuffer(data_buff_sizes[i]).ValueOrDie();
                offset_buffs[i] = arrow::AllocateBuffer(offset_buff_sizes[i]).ValueOrDie();

                segments[i*2].first = (void*)data_buffs[i]->mutable_data();
                segments[i*2].second = data_buff_sizes[i];

                segments[(i*2)+1].first = (void*)offset_buffs[i]->mutable_data();
                segments[(i*2)+1].second = offset_buff_sizes[i];
            }

            tl::bulk local = ctx.engine.expose(segments, tl::bulk_mode::write_only);
            b.on(req.get_endpoint()) >> local;

            for (int64_t i = 0; i < num_cols; i++) {
                std::shared_ptr<arrow::DataType> type = type_from_id(types[i]);  
                if (is_binary_like(type->id())) {
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buffs[i]), std::move(data_buffs[i]));
                    columns.push_back(col_arr);
                } else {
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buffs[i]));
                    columns.push_back(col_arr);
                }
            }

            batch = arrow::RecordBatch::Make(schema, num_rows, columns);
            return req.respond(0);
        };
    ctx.engine.define("do_rdma", f);
    tl::remote_procedure get_next_batch = ctx.engine.define("get_next_batch");

    int e = get_next_batch.on(ctx.endpoint)(uuid);
    if (e == 0) {
        return batch;
    } else {
        return nullptr;
    }
}

arrow::Status Main(char **argv) {
    auto filter = 
        cp::greater(cp::field_ref("total_amount"), cp::literal(10));
    
    auto schema = arrow::schema({arrow::field("passenger_count", arrow::int64()),
                                 arrow::field("fair_amount", arrow::float64())});

    ConnCtx ctx = Init(argv[1]);
    ARROW_ASSIGN_OR_RAISE(auto req, GetScanRequest(filter, schema));
    std::string uuid = Scan(ctx, req);

    std::shared_ptr<arrow::RecordBatch> batch;
    while ((batch = GetNextBatch(ctx, uuid).ValueOrDie()) != nullptr) {
        std::cout << batch->ToString();
    }
}

int main(int argc, char** argv) {
    Main(argv);
    exit(0);
}
