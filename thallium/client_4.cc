#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>

#include <thallium.hpp>

#include "arrow_headers.h"
#include "payload.h"


namespace tl = thallium;
namespace cp = arrow::compute;


const int32_t kTransferSize = 19 * 1024 * 1024;


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
    tl::remote_procedure get_next_batch = conn_ctx.engine.define("get_next_batch");
    ScanRespStub resp = get_next_batch.on(conn_ctx.endpoint)(scan_ctx.uuid);

    if (resp.batch_sizes.size() == 0) {
        return std::vector<std::shared_ptr<arrow::RecordBatch>>();
    }

    if (flag == 1) {
        segments[0].first = (uint8_t*)malloc(kTransferSize);
        segments[0].second = kTransferSize;
        local = conn_ctx.engine.expose(segments, tl::bulk_mode::write_only);
    }

    resp.bulk(0, resp.total_size).on(conn_ctx.endpoint) >> local(0, resp.total_size);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;    
    int num_cols = scan_ctx.schema->num_fields();            
    for (int32_t batch_idx = 0; batch_idx < resp.batch_sizes.size(); batch_idx++) {
        int32_t num_rows = resp.batch_sizes[batch_idx];
        std::shared_ptr<arrow::RecordBatch> batch;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        
        for (int64_t i = 0; i < num_cols; i++) {
            int32_t magic_off = (batch_idx * num_cols) + i;
            std::shared_ptr<arrow::DataType> type = scan_ctx.schema->field(i)->type();  
            if (is_binary_like(type->id())) {
                std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                    (uint8_t*)segments[0].first + resp.data_offsets[magic_off], resp.data_sizes[magic_off]
                );
                std::shared_ptr<arrow::Buffer> offset_buff = arrow::Buffer::Wrap(
                    (uint8_t*)segments[0].first + resp.off_offsets[magic_off], resp.off_sizes[magic_off]
                );

                std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::StringArray>(num_rows, std::move(offset_buff), std::move(data_buff));
                columns.push_back(col_arr);
            } else {
                std::shared_ptr<arrow::Buffer> data_buff = arrow::Buffer::Wrap(
                    (uint8_t*)segments[0].first  + resp.data_offsets[magic_off], resp.data_sizes[magic_off]
                );
                std::shared_ptr<arrow::Array> col_arr = std::make_shared<arrow::PrimitiveArray>(type, num_rows, std::move(data_buff));
                columns.push_back(col_arr);
            }
        }
        batch = arrow::RecordBatch::Make(scan_ctx.schema, num_rows, columns);
        batches.push_back(batch);
    }
    return batches;
}

arrow::Status Main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "./tc [uri]" << std::endl;
        exit(1);
    }

    std::string uri = argv[1];

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

    ConnCtx conn_ctx = Init("ofi+verbs", uri);
    int64_t total_rows = 0;
    int64_t total_batches = 0;

    std::string path = "/mnt/cephfs/dataset";
    ARROW_ASSIGN_OR_RAISE(auto scan_req, GetScanRequest(path, filter, schema, schema));
    ScanCtx scan_ctx = Scan(conn_ctx, scan_req);
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto start = std::chrono::high_resolution_clock::now();
    while ((batches = GetNextBatch(conn_ctx, scan_ctx, (total_rows == 0))).size() != 0) {
        total_batches += batches.size();
        for (auto batch : batches) {
            total_rows += batch->num_rows();
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Read " << total_rows << " rows in " << std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) << " ms" << std::endl;
    conn_ctx.engine.finalize();
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    Main(argc, argv);
}
