#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

#include "payload.h"


namespace cp = arrow::compute;


class RandomAccessObject : public arrow::io::RandomAccessFile {
 public:
  explicit RandomAccessObject(uint8_t *ptr, int64_t size) {
    file_ptr = ptr;
    file_size = size;
  }

  ~RandomAccessObject() override { DCHECK_OK(Close()); }

  arrow::Status CheckClosed() const {
    if (closed_) {
      return arrow::Status::Invalid("Operation on closed stream");
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return arrow::Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > file_size) {
      return arrow::Status::IOError("Cannot ", action, " past end of file");
    }
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    return arrow::Status::NotImplemented(
        "ReadAt has not been implemented in RandomAccessObject");
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, file_size - position);

    if (nbytes > 0) {
        return std::make_shared<arrow::Buffer>(file_ptr + position, nbytes);
    }
    return std::make_shared<arrow::Buffer>("");
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  arrow::Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return file_size;
  }

  arrow::Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const override { return closed_; }

 private:
  bool closed_ = false;
  int64_t pos_ = 0;
  uint8_t *file_ptr = NULL;
  int64_t file_size = -1;
};


arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanBenchmark(cp::ExecContext& exec_context, const ScanReqRPCStub& stub) {
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
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

    auto im_ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner_builder, im_ds->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner, im_ds_scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, im_ds_scanner->ToRecordBatchReader());

    return reader;
}


arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanEXT4(const ScanReqRPCStub& stub) {   
    // deserialize filter
    ARROW_ASSIGN_OR_RAISE(auto filter,
      arrow::compute::Deserialize(std::make_shared<arrow::Buffer>(
      stub.filter_buffer, stub.filter_buffer_size))
    );

    // deserialize schemas
    arrow::ipc::DictionaryMemo empty_memo;
    arrow::io::BufferReader projection_schema_reader(stub.projection_schema_buffer,
                                                     stub.projection_schema_buffer_size);
    arrow::io::BufferReader dataset_schema_reader(stub.dataset_schema_buffer,
                                                  stub.dataset_schema_buffer_size);
    ARROW_ASSIGN_OR_RAISE(auto projection_schema,
                          arrow::ipc::ReadSchema(&projection_schema_reader, &empty_memo));

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
                          arrow::ipc::ReadSchema(&dataset_schema_reader, &empty_memo));

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(stub.path));
    arrow::dataset::FileSource source(file);
    ARROW_ASSIGN_OR_RAISE(
        auto fragment, format->MakeFragment(std::move(source), arrow::compute::literal(true)));
    
    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    auto scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(
        dataset_schema, std::move(fragment), std::move(options));

    ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    ARROW_RETURN_NOT_OK(scanner_builder->Project(projection_schema->field_names()));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
    return reader;
}


arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanEXT4MMap(const ScanReqRPCStub& stub) {   
    // deserialize filter
    ARROW_ASSIGN_OR_RAISE(auto filter,
      arrow::compute::Deserialize(std::make_shared<arrow::Buffer>(
      stub.filter_buffer, stub.filter_buffer_size))
    );

    // deserialize schemas
    arrow::ipc::DictionaryMemo empty_memo;
    arrow::io::BufferReader projection_schema_reader(stub.projection_schema_buffer,
                                                     stub.projection_schema_buffer_size);
    arrow::io::BufferReader dataset_schema_reader(stub.dataset_schema_buffer,
                                                  stub.dataset_schema_buffer_size);
    ARROW_ASSIGN_OR_RAISE(auto projection_schema,
                          arrow::ipc::ReadSchema(&projection_schema_reader, &empty_memo));

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
                          arrow::ipc::ReadSchema(&dataset_schema_reader, &empty_memo));

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::MemoryMappedFile::Open(stub.path, arrow::io::FileMode::READ));
    arrow::dataset::FileSource source(file);
    ARROW_ASSIGN_OR_RAISE(
        auto fragment, format->MakeFragment(std::move(source), arrow::compute::literal(true)));
    
    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    auto scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(
        dataset_schema, std::move(fragment), std::move(options));

    ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    ARROW_RETURN_NOT_OK(scanner_builder->Project(projection_schema->field_names()));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
    return reader;
}


arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ScanBake(const ScanReqRPCStub& stub, uint8_t *ptr) {   
    // deserialize filter
    ARROW_ASSIGN_OR_RAISE(auto filter,
      arrow::compute::Deserialize(std::make_shared<arrow::Buffer>(
      stub.filter_buffer, stub.filter_buffer_size))
    );

    // deserialize schemas
    arrow::ipc::DictionaryMemo empty_memo;
    arrow::io::BufferReader projection_schema_reader(stub.projection_schema_buffer,
                                                     stub.projection_schema_buffer_size);
    arrow::io::BufferReader dataset_schema_reader(stub.dataset_schema_buffer,
                                                  stub.dataset_schema_buffer_size);
    ARROW_ASSIGN_OR_RAISE(auto projection_schema,
                          arrow::ipc::ReadSchema(&projection_schema_reader, &empty_memo));

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
                          arrow::ipc::ReadSchema(&dataset_schema_reader, &empty_memo));

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    auto file = std::make_shared<RandomAccessObject>(ptr, 16074327);
    arrow::dataset::FileSource source(file);
    ARROW_ASSIGN_OR_RAISE(
        auto fragment, format->MakeFragment(std::move(source), arrow::compute::literal(true)));
    
    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    auto scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(
        dataset_schema, std::move(fragment), std::move(options));

    ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    ARROW_RETURN_NOT_OK(scanner_builder->Project(projection_schema->field_names()));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
    return reader;
}
