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

const int32_t BUFFER_SIZE = 1024*1024;

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

std::vector<void*> pointers(34);
std::vector<std::pair<void*,std::size_t>> segments(34);
tl::bulk local;

ScanCtx Scan(ConnCtx &conn_ctx, ScanReq &scan_req) {
    tl::remote_procedure scan = conn_ctx.engine.define("scan");
    ScanCtx scan_ctx;
    std::string uuid = scan.on(conn_ctx.endpoint)(scan_req.stub);
    scan_ctx.uuid = uuid;
    scan_ctx.schema = scan_req.schema;
    for (int i = 0; i < segments.size(); i++) {
        pointers[i] = (void*)malloc(BUFFER_SIZE);
        segments[i] = std::make_pair(pointers[i], BUFFER_SIZE);
    }
    return scan_ctx;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch(ConnCtx &conn_ctx, ScanCtx &scan_ctx, int flag) {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::function<void(const tl::request&, int64_t&, std::vector<int64_t>&, std::vector<int64_t>&, tl::bulk&)> f =
        [&conn_ctx, &scan_ctx, &batch, &segments, &pointers, &flag, &local](const tl::request& req, int64_t& num_rows, std::vector<int64_t>& data_buff_sizes, std::vector<int64_t>& offset_buff_sizes, tl::bulk& b) {
            int num_cols = scan_ctx.schema->num_fields();

            {
                for (int i = 0; i < num_cols; i++) {
                    std::cout << "data_buff_sizes[" << i << "] = " << data_buff_sizes[i] << std::endl;
                    std::cout << "offset_buff_sizes[" << i << "] = " << offset_buff_sizes[i] << std::endl;
                }
            }

            std::vector<std::shared_ptr<arrow::Array>> columns;
            if (flag == 1) {
                std::cout << "Pinning client side buffers" << std::endl;
                {
                    MeasureExecutionTime m("memory_allocate");
                }

                {
                    MeasureExecutionTime m("client_expose");
                    local = conn_ctx.engine.expose(segments, tl::bulk_mode::write_only);
                }
            }

            {
                MeasureExecutionTime m("RDMA");
                b.on(req.get_endpoint()) >> local;
            }

            for (int64_t i = 0; i < num_cols; i++) {
                std::shared_ptr<arrow::DataType> type = scan_ctx.schema->field(i)->type();  
                if (is_binary_like(type->id())) {
                    std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[i*2].first, data_buff_sizes[i]
                    );
                    std::cout << "XXXXX: " << offset_buff_sizes[i] << std::endl;
                    std::shared_ptr<arrow::Buffer> offset_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[(i*2)+1].first, offset_buff_sizes[i]
                    );

                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(data_buff), std::move(offset_buff));
                    columns.push_back(col_arr);
                } else {
                    std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                        (uint8_t*)segments[i*2].first, data_buff_sizes[i]
                    );
                    std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buff));
                    columns.push_back(col_arr);
                }
            }

            batch = arrow::RecordBatch::Make(scan_ctx.schema, num_rows, columns);
            return req.respond(0);
        };
    conn_ctx.engine.define("do_rdma", f);
    tl::remote_procedure get_next_batch = conn_ctx.engine.define("get_next_batch");
    int e;
    {
        MeasureExecutionTime m("get_next_batch");
        e = get_next_batch.on(conn_ctx.endpoint)(scan_ctx.uuid);
    }
    if (e == 0) {
        return batch;
    } else {
        return nullptr;
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
    int64_t num_batches = 0;

    std::string path = "/mnt/cephfs/dataset";
    ScanReq scan_req;
    ARROW_ASSIGN_OR_RAISE(scan_req, GetScanRequest(path, filter, schema, schema));
    ScanCtx scan_ctx = Scan(conn_ctx, scan_req);
    std::shared_ptr<arrow::RecordBatch> batch;
    {
        MEASURE_FUNCTION_EXECUTION_TIME
        while ((batch = GetNextBatch(conn_ctx, scan_ctx, (total_rows == 0)).ValueOrDie()) != nullptr) {
            total_rows += batch->num_rows();
            num_batches++;
            std::cout << batch->ToString() << std::endl;       
        }
    }
    std::cout << "Read " << total_rows << " rows in " << num_batches << " batches" << std::endl;
    
    conn_ctx.engine.finalize();
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    Main(argc, argv);
}
