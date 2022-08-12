#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"

class ParquetStorageService : public arrow::flight::FlightServerBase {
 public:
  explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> fs, std::string host, int32_t port)
      : fs_(std::move(fs)), host_(host), port_(port) {}

  int32_t Port() { return port_; }

  arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                              const arrow::flight::FlightDescriptor& descriptor,
                              std::unique_ptr<arrow::flight::FlightInfo>* info) {
    ARROW_ASSIGN_OR_RAISE(auto file_info, fs_->GetFileInfo(descriptor.path[0]));
    ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
    *info = std::unique_ptr<arrow::flight::FlightInfo>(
        new arrow::flight::FlightInfo(std::move(flight_info)));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket& request,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
    // std::string path;
    // ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(request.ticket, &path)); 
    // auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
      
    // arrow::fs::FileSelector s;
    // s.base_dir = std::move(path);
    // s.recursive = true;

    auto filter = 
        arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                arrow::compute::literal(-200));

    // arrow::dataset::FileSystemFactoryOptions options;
    // ARROW_ASSIGN_OR_RAISE(auto factory, 
    //   arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
    // arrow::dataset::FinishOptions finish_options;
    // ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

    // ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    // ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    // ARROW_RETURN_NOT_OK(scanner_builder->Project({"passenger_count", "fare_amount"}));

    // ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    // ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

    // auto im_ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    // ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner_builder, im_ds->NewScan());
    // ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner, im_ds_scanner_builder->Finish());
    // ARROW_ASSIGN_OR_RAISE(auto reader, im_ds_scanner->ToRecordBatchReader());

    // *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
    //     new arrow::flight::RecordBatchStream(reader));
  
    // return arrow::Status::OK();
    auto dataset_schema = arrow::schema({
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

    std::cout << request.ticket << std::endl;

    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::MemoryMappedFile::Open(request.ticket, arrow::io::FileMode::READ));
    arrow::dataset::FileSource source(file);
    ARROW_ASSIGN_OR_RAISE(
        auto fragment, format->MakeFragment(std::move(source), arrow::compute::literal(true)));
    
    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    auto scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(
        dataset_schema, std::move(fragment), std::move(options));

    ARROW_RETURN_NOT_OK(scanner_builder->Filter(filter));
    ARROW_RETURN_NOT_OK(scanner_builder->Project({"passenger_count", "fare_amount"}));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());

    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
        new arrow::flight::RecordBatchStream(reader));
  
    return arrow::Status::OK();
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
      const arrow::fs::FileInfo& file_info) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({});
    std::string path = "file://" + file_info.path();
    auto descriptor = arrow::flight::FlightDescriptor::Path({path});

    arrow::flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = path;
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(
        arrow::flight::Location::ForGrpcTcp(host_, port(), &location));
    endpoint.locations.push_back(location);

    return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
  }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::string host_;
  int32_t port_;
};

int main(int argc, char *argv[]) {
  std::string host = "10.10.1.2";
  int32_t port = (int32_t)std::stoi(argv[1]);
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  arrow::flight::Location server_location;
  arrow::flight::Location::ForGrpcTcp(host, port, &server_location);

  arrow::flight::FlightServerOptions options(server_location);
  auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
      new ParquetStorageService(std::move(fs), host, port));
  server->Init(options);
  std::cout << "Listening on port " << server->port() << std::endl;
  server->Serve();
}