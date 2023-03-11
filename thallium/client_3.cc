#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include <arrow/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/testing/random.h>
#include <arrow/util/key_value_metadata.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <thallium.hpp>

#include "payload.h"

// 0 1 2   3 4 5
// 0       1  

void see_vector(std::vector<int32_t> v, std::string s) {
    std::cout << s << std::endl;
    std::cout << "Size: " << v.size() << std::endl;
    for (auto i : v) {
        std::cout << i << " ";
    }
    std::cout << "\n\n\n";
}

class MeasureExecutionTime{
    private:
        const std::chrono::steady_clock::time_point begin;
        const std::string caller;
        // std::ofstream log;
    public:
        MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()) {
            // log.open("result_client.txt", std::ios_base::app);
        }
        
        ~MeasureExecutionTime() {
            const auto duration=std::chrono::steady_clock::now()-begin;
            std::string s = caller + " : " + std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count()/1000) + "\n";
            std::cout << s;
            // log << s;
            // log.close();
        }
};

#ifndef MEASURE_FUNCTION_EXECUTION_TIME
#define MEASURE_FUNCTION_EXECUTION_TIME const MeasureExecutionTime measureExecutionTime(__FUNCTION__);
#endif

namespace tl = thallium;
namespace cp = arrow::compute;

arrow::Result<ScanReq> GetScanRequest(std::string path,
                                      cp::Expression filter, 
                                      std::shared_ptr<arrow::Schema> projection_schema,
                                      std::shared_ptr<arrow::Schema> dataset_schema) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> filter_buff, arrow::compute::Serialize(filter));
    ARROW_ASSIGN_OR_RAISE(auto projection_schema_buff, arrow::ipc::SerializeSchema(*projection_schema));
    ARROW_ASSIGN_OR_RAISE(auto dataset_schema_buff, arrow::ipc::SerializeSchema(*dataset_schema));
    ScanReqRPCStub stub(
        path,
        const_cast<uint8_t*>(filter_buff->data()), filter_buff->size(), 
        const_cast<uint8_t*>(dataset_schema_buff->data()), dataset_schema_buff->size(),
        const_cast<uint8_t*>(projection_schema_buff->data()), projection_schema_buff->size()
    );
    ScanReq req;
    req.stub = stub;
    req.schema = projection_schema;
    return req;
}

ConnCtx Init(std::string protocol, std::string host) {
    ConnCtx ctx;
    tl::engine engine(protocol, THALLIUM_SERVER_MODE, true);
    tl::endpoint endpoint = engine.lookup(host);
    ctx.engine = engine;
    ctx.endpoint = endpoint;
    return ctx;
}

tl::bulk local;
std::vector<std::pair<void*,std::size_t>> segments(1);

ScanCtx Scan(ConnCtx &conn_ctx, ScanReq &scan_req) {
    tl::remote_procedure scan = conn_ctx.engine.define("scan");
    ScanCtx scan_ctx;
    std::string uuid = scan.on(conn_ctx.endpoint)(scan_req.stub);
    scan_ctx.uuid = uuid;
    scan_ctx.schema = scan_req.schema;
    return scan_ctx;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> GetNextBatch(ConnCtx &conn_ctx, ScanCtx &scan_ctx, int32_t flag) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    std::function<void(const tl::request&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, std::vector<int32_t>&, int32_t&, tl::bulk&)> f =
        [&conn_ctx, &scan_ctx, &batches, &segments, &local, &flag](const tl::request& req, std::vector<int32_t> &batch_sizes, std::vector<int32_t>& batch_offsets, std::vector<int32_t>& data_offsets, std::vector<int32_t>& data_sizes, std::vector<int32_t>& off_offsets, std::vector<int32_t>& off_sizes, int32_t& total_size, tl::bulk& b) {
            if (flag == 1) {
                std::cout << "Start exposing" << std::endl;
                {
                    MeasureExecutionTime m("memory_allocate");
                    segments[0].first = (uint8_t*)malloc(20*1024*1024);
                    segments[0].second = 20*1024*1024;
                }

                {
                    MeasureExecutionTime m("client_expose");
                    local = conn_ctx.engine.expose(segments, tl::bulk_mode::write_only);
                }
            }

            see_vector(batch_sizes, "batch_sizes");
            see_vector(batch_offsets,  "batch_offsets");
            see_vector(data_offsets, "data_offsets");
            see_vector(data_sizes, "data_sizes");
            see_vector(off_offsets, "off_offsets");
            see_vector(off_sizes,   "off_sizes");
            
            int num_cols = scan_ctx.schema->num_fields();
                        
            {
                MeasureExecutionTime m("RDMA");
                b(0, total_size).on(req.get_endpoint()) >> local(0, total_size);
            }
            
            for (int32_t batch_idx = 0; batch_idx < batch_sizes.size(); batch_idx++) {
                int32_t num_rows = batch_sizes[batch_idx];
                int32_t batch_offset = batch_offsets[batch_idx];
                
                std::shared_ptr<arrow::RecordBatch> batch;
                std::vector<std::shared_ptr<arrow::Array>> columns;
                
                for (int64_t i = 0; i < num_cols; i++) {
                    int32_t magic_off = (batch_idx * num_cols) + i;
                    std::shared_ptr<arrow::DataType> type = scan_ctx.schema->field(i)->type();  
                    if (is_binary_like(type->id())) {
                        std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                            (uint8_t*)segments[0].first + batch_offset + data_offsets[magic_off], data_sizes[magic_off]
                        );
                        std::shared_ptr<arrow::Buffer> offset_buff = arrow::Buffer::Wrap(
                            (uint8_t*)segments[0].first + batch_offset + off_offsets[magic_off], off_sizes[magic_off]
                        );

                        std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buff), std::move(data_buff));
                        columns.push_back(col_arr);
                    } else {
                        std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                            (uint8_t*)segments[0].first + batch_offset + data_offsets[magic_off], data_sizes[magic_off]
                        );
                        std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buff));
                        columns.push_back(col_arr);
                    }
                }
                batch = arrow::RecordBatch::Make(scan_ctx.schema, num_rows, columns);

                batches.push_back(batch);
            }

            return req.respond(0);
        };
    conn_ctx.engine.define("do_rdma", f);
    tl::remote_procedure get_next_batch = conn_ctx.engine.define("get_next_batch");

    int e;
    
    {
        MeasureExecutionTime m("get_next_batch_rpc");
        e = get_next_batch.on(conn_ctx.endpoint)(scan_ctx.uuid);
    }

    if (e == 0) {
        return batches;
    } else {
        return std::vector<std::shared_ptr<arrow::RecordBatch>>();
    }
}

arrow::Status Main(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "./tc [uri] [backend] [protocol]" << std::endl;
        exit(1);
    }

    std::string uri = argv[1];
    std::string backend = argv[2];
    std::string protocol = argv[3];

    auto filter = 
        cp::greater(cp::field_ref("total_amount"), cp::literal(-200));

    auto schema = arrow::schema({
        arrow::field("VendorID", arrow::int64()),
        arrow::field("tpep_pickup_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("tpep_dropoff_datetime", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("passenger_count", arrow::int64()),
        arrow::field("trip_distance", arrow::float64()),
        arrow::field("RatecodeID", arrow::int64()),
        arrow::field("store_and_fwd_flag", arrow::utf8()),
        arrow::field("PULocationID", arrow::int64()),
        arrow::field("DOLocationID", arrow::int64()),
        arrow::field("payment_type", arrow::int64()),
        arrow::field("fare_amount", arrow::float64()),
        arrow::field("extra", arrow::float64()),
        arrow::field("mta_tax", arrow::float64()),
        arrow::field("tip_amount", arrow::float64()),
        arrow::field("tolls_amount", arrow::float64()),
        arrow::field("improvement_surcharge", arrow::float64()),
        arrow::field("total_amount", arrow::float64())
    });

    ConnCtx conn_ctx = Init(protocol, uri);
    int64_t total_rows = 0;
    int64_t total_batches = 0;

    if (backend == "dataset") {
        std::string path = "/mnt/cephfs/dataset";
        ARROW_ASSIGN_OR_RAISE(auto scan_req, GetScanRequest(path, filter, schema, schema));
        ScanCtx scan_ctx = Scan(conn_ctx, scan_req);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        {
            MEASURE_FUNCTION_EXECUTION_TIME
            while ((batches = GetNextBatch(conn_ctx, scan_ctx, (total_rows == 0))).size() != 0) {
                for (auto batch : batches) {
                    total_rows += batch->num_rows();
                    // std::cout << batch->ToString() << std::endl;
                    break;
                }
                total_batches += batches.size();
            }
        }
        std::cout << "Read " << total_rows << " rows in " << total_batches << std::endl;
    }

    conn_ctx.engine.finalize();
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    Main(argc, argv);
}