#include <iostream>
#include <unordered_map>
#include <fstream>
#include <mutex>
#include <condition_variable>

#include <arrow/api.h>
#include <arrow/compute/expression.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/testing/random.h>
#include <arrow/util/key_value_metadata.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <thallium.hpp>

#include "ace.h"

// copy of 3

namespace tl = thallium;
namespace cp = arrow::compute;


const int32_t kTransferSize = 19 * 1024 * 1024;
const int32_t kBatchSize = 1 << 17;

int64_t total_produced_rows = 0;
int64_t total_consumed_rows = 0;

class ConcurrentRecordBatchQueue {
    public:
        std::deque<std::shared_ptr<arrow::RecordBatch>> queue;
        std::mutex mutex;
        std::condition_variable cv;
    
        void push_back(std::shared_ptr<arrow::RecordBatch> batch) {
            std::unique_lock<std::mutex> lock(mutex);
            queue.push_back(batch);
            lock.unlock();
            cv.notify_one();
        }

        void wait_n_pop(std::shared_ptr<arrow::RecordBatch> &batch) {
            std::unique_lock<std::mutex> lock(mutex);
            while (queue.empty()) {
                cv.wait(lock);
            }
            batch = queue.front();
            queue.pop_front();
        }
};


ConcurrentRecordBatchQueue cq;
void scan_handler(void *arg) {
    arrow::RecordBatchReader *reader = (arrow::RecordBatchReader*)arg;
    std::shared_ptr<arrow::RecordBatch> batch;

    reader->ReadNext(&batch);
    cq.push_back(batch);
    total_produced_rows += batch->num_rows();
    
    while (batch != nullptr) {
        reader->ReadNext(&batch);
        cq.push_back(batch);
        total_produced_rows += batch->num_rows();
    }
    
    cq.push_back(nullptr);
    std::cout << "Finished producing" << std::endl;
}


int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "./ts [selectivity] [backend]" << std::endl;
        exit(1);
    }

    std::string selectivity = argv[1];
    std::string backend = argv[2];

    tl::engine engine("ofi+verbs", THALLIUM_SERVER_MODE, true);
    margo_instance_id mid = engine.get_margo_instance();
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    tl::remote_procedure do_rdma = engine.define("do_rdma");
    uint8_t *segment_buffer = (uint8_t*)malloc(kTransferSize);
    
    std::vector<std::pair<void*,std::size_t>> segments(1);
    tl::bulk arrow_bulk;
    
    // create a new pool
    // tl::managed<tl::pool> new_pool = tl::pool::create(tl::pool::access::spmc);
    tl::managed<tl::xstream> xstream = 
        tl::xstream::create(tl::scheduler::predef::deflt, engine.get_progress_pool());
    
    std::function<void(const tl::request&, const ScanReqRPCStub&)> scan = 
        [&xstream, &cq, &backend, &engine, &do_rdma, &selectivity, &total_consumed_rows, &segment_buffer, &segments, &arrow_bulk](const tl::request &req, const ScanReqRPCStub& stub) {
            arrow::dataset::internal::Initialize();
            cp::ExecContext exec_ctx;
            std::shared_ptr<arrow::RecordBatchReader> reader = ScanDataset(exec_ctx, stub, backend, selectivity).ValueOrDie();
            bool finished = false;
            auto start = std::chrono::high_resolution_clock::now();

            segments[0].first = (void*)segment_buffer;
            segments[0].second = kTransferSize;
            arrow_bulk = engine.expose(segments, tl::bulk_mode::read_write);
            
            xstream->make_thread([&]() {
                scan_handler((void*)reader.get());
            }, tl::anonymous());

            std::shared_ptr<arrow::RecordBatch> new_batch;

            while (1 && !finished) {
                std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
                std::vector<int32_t> batch_sizes;
                int64_t rows_processed = 0;

                while (rows_processed < kBatchSize) {
                    cq.wait_n_pop(new_batch);
                    total_consumed_rows += new_batch->num_rows();
                    if (new_batch == nullptr) {
                        std::cout << "Finished reading" << std::endl;
                        finished = true;
                        break;
                    }

                    rows_processed += new_batch->num_rows();

                    batches.push_back(new_batch);
                    batch_sizes.push_back(new_batch->num_rows());
                } 

                if (batches.size() != 0) {
                    int32_t curr_pos = 0;
                    int32_t total_size = 0;

                    std::vector<int32_t> data_offsets;
                    std::vector<int32_t> data_sizes;

                    std::vector<int32_t> off_offsets;
                    std::vector<int32_t> off_sizes;

                    std::string null_buff = "x";
                    
                    for (auto b : batches) {
                        for (int32_t i = 0; i < b->num_columns(); i++) {
                            std::shared_ptr<arrow::Array> col_arr = b->column(i);
                            arrow::Type::type type = col_arr->type_id();
                            int32_t null_count = col_arr->null_count();

                            if (is_binary_like(type)) {
                                std::shared_ptr<arrow::Buffer> data_buff = 
                                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
                                std::shared_ptr<arrow::Buffer> offset_buff = 
                                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();

                                int32_t data_size = data_buff->size();
                                int32_t offset_size = offset_buff->size();

                                data_offsets.emplace_back(curr_pos);
                                data_sizes.emplace_back(data_size); 
                                memcpy(segment_buffer + curr_pos, data_buff->data(), data_size);
                                curr_pos += data_size;

                                off_offsets.emplace_back(curr_pos);
                                off_sizes.emplace_back(offset_size);
                                memcpy(segment_buffer + curr_pos, offset_buff->data(), offset_size);
                                curr_pos += offset_size;

                                total_size += (data_size + offset_size);
                            } else {
                                std::shared_ptr<arrow::Buffer> data_buff = 
                                    std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();

                                int32_t data_size = data_buff->size();
                                int32_t offset_size = null_buff.size(); 

                                data_offsets.emplace_back(curr_pos);
                                data_sizes.emplace_back(data_size);
                                memcpy(segment_buffer + curr_pos, data_buff->data(), data_size);
                                curr_pos += data_size;

                                off_offsets.emplace_back(curr_pos);
                                off_sizes.emplace_back(offset_size);
                                memcpy(segment_buffer + curr_pos, (uint8_t*)null_buff.c_str(), offset_size);
                                curr_pos += offset_size;

                                total_size += (data_size + offset_size);
                            }
                        }
                    }

                    segments[0].second = total_size;
                    do_rdma.on(req.get_endpoint())(batch_sizes, data_offsets, data_sizes, off_offsets, off_sizes, total_size, arrow_bulk);
                    std::cout << "Total produced rows: " << total_produced_rows << std::endl;
                    std::cout << "Total consumed rows: " << total_consumed_rows << std::endl;
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Server side logic took: " << std::to_string((double)std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/1000) << " ms" << std::endl;

            return req.respond(0);
        };

    // int32_t total_rows_written = 0;
    // std::function<void(const tl::request&, const std::string&)> get_next_batch = 
    //     [&mid, &svr_addr, &engine, &do_rdma, &reader_map, &total_rows_written, &segment_buffer, &segments, &arrow_bulk](const tl::request &req, const std::string& uuid) {
    //         std::shared_ptr<arrow::RecordBatchReader> reader = reader_map[uuid];
    //         std::shared_ptr<arrow::RecordBatch> batch;
    //         std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    //         std::vector<int32_t> batch_sizes;

    //         if (total_rows_written == 0) {
    //             segments[0].first = (void*)segment_buffer;
    //             segments[0].second = kTransferSize;
    //             arrow_bulk = engine.expose(segments, tl::bulk_mode::read_write);
    //         }

    //         // collect about 1<<17 batches for each transfer
    //         int32_t total_rows_in_transfer_batch = 0;
    //         while (total_rows_in_transfer_batch < 131072) {
    //             reader->ReadNext(&batch);
    //             if (batch == nullptr) {
    //                 break;
    //             }
    //             batches.push_back(batch);
    //             batch_sizes.push_back(batch->num_rows());
    //             total_rows_in_transfer_batch += batch->num_rows();
    //         }

            
    //             return req.respond(0);
    //         } else {
    //             reader_map.erase(uuid);
    //             return req.respond(1);
    //         }
    //     };
    
    engine.define("scan", scan);
    std::ofstream file("/tmp/thallium_uri");
    file << engine.self();
    file.close();
    std::cout << "Server running at address " << engine.self() << std::endl;    

    engine.wait_for_finalize();        
};