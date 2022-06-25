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

#include "payload.h"


class MeasureExecutionTime{
  private:
      const std::chrono::steady_clock::time_point begin;
      const std::string caller;
  public:
      MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()){}
      ~MeasureExecutionTime(){
          const auto duration=std::chrono::steady_clock::now()-begin;
          std::cout << (double)std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()/1000<<std::endl;
      }
};

#ifndef MEASURE_FUNCTION_EXECUTION_TIME
#define MEASURE_FUNCTION_EXECUTION_TIME const MeasureExecutionTime measureExecutionTime(__FUNCTION__);
#endif


namespace tl = thallium;
namespace cp = arrow::compute;


arrow::Result<ScanReq> GetScanRequest(cp::Expression filter, std::shared_ptr<arrow::Schema> schema) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> filter_buff, arrow::compute::Serialize(filter));
    ARROW_ASSIGN_OR_RAISE(auto projection_buff, arrow::ipc::SerializeSchema(*schema));
    ScanReqRPCStub stub(
        const_cast<uint8_t*>(filter_buff->data()), filter_buff->size(), 
        const_cast<uint8_t*>(projection_buff->data()), projection_buff->size()
    );
    ScanReq req;
    req.stub = stub;
    req.filter = filter;
    req.schema = schema;
    return req;
}

ConnCtx Init(std::string host) {
    ConnCtx ctx;
    tl::engine engine("verbs", THALLIUM_SERVER_MODE);
    tl::endpoint endpoint = engine.lookup(host);
    ctx.engine = engine;
    ctx.endpoint = endpoint;
    return ctx;
}

ScanCtx Scan(ConnCtx &conn_ctx, ScanReq &scan_req) {
    tl::remote_procedure scan = conn_ctx.engine.define("scan");
    ScanCtx scan_ctx;
    std::string uuid = scan.on(conn_ctx.endpoint)(scan_req.stub);
    scan_ctx.uuid = uuid;
    scan_ctx.schema = scan_req.schema;
    return scan_ctx;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch(ConnCtx &conn_ctx, ScanCtx &scan_ctx) {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::function<void(const tl::request&, int64_t&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> f =
        [&conn_ctx, &scan_ctx, &batch](const tl::request& req, int64_t& num_rows, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk& b) {
            int num_cols = scan_ctx.schema->num_fields();
            
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

            tl::bulk local = conn_ctx.engine.expose(segments, tl::bulk_mode::write_only);
            b.on(req.get_endpoint()) >> local;

            for (int64_t i = 0; i < num_cols; i++) {
                std::shared_ptr<arrow::DataType> type = scan_ctx.schema->field(i)->type();  
                if (is_binary_like(type->id())) {
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buffs[i]), std::move(data_buffs[i]));
                    columns.push_back(col_arr);
                } else {
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buffs[i]));
                    columns.push_back(col_arr);
                }
            }

            batch = arrow::RecordBatch::Make(scan_ctx.schema, num_rows, columns);
            return req.respond(0);
        };
    conn_ctx.engine.define("do_rdma", f);
    tl::remote_procedure get_next_batch = conn_ctx.engine.define("get_next_batch");

    int e = get_next_batch.on(conn_ctx.endpoint)(scan_ctx.uuid);
    if (e == 0) {
        return batch;
    } else {
        return nullptr;
    }
}

arrow::Status Main(char **argv) {
    auto filter = 
        cp::greater(cp::field_ref("total_amount"), cp::literal(-200));
    
    auto schema = arrow::schema({arrow::field("passenger_count", arrow::int64()),
                                 arrow::field("fare_amount", arrow::float64())});

    std::string uri_base = "ofi+verbs;ofi_rxm://10.10.1.2:";
    std::string uri = uri_base + argv[1];

    ConnCtx conn_ctx = Init(uri);
    ARROW_ASSIGN_OR_RAISE(auto scan_req, GetScanRequest(filter, schema));
    ScanCtx scan_ctx = Scan(conn_ctx, scan_req);
    int64_t total_rows = 0;
    std::shared_ptr<arrow::RecordBatch> batch;
    {
        MEASURE_FUNCTION_EXECUTION_TIME
        while ((batch = GetNextBatch(conn_ctx, scan_ctx).ValueOrDie()) != nullptr) {
            total_rows += batch->num_rows();
        }
    }
    std::cout << "Read " << total_rows << " rows" << std::endl;
    exit(0);
}

int main(int argc, char** argv) {
    Main(argv);
}
