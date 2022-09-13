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
        explicit ParquetStorageService(
            std::shared_ptr<arrow::fs::FileSystem> fs, std::string host, int32_t port, 
            std::string selectivity, std::string backend
        ) : fs_(std::move(fs)), host_(host), port_(port), selectivity_(selectivity), backend_(backend) {}

        int32_t Port() { return port_; }

        arrow::compute::Expression GetFilter() {
            if (selectivity_ == "100") {
                return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                               arrow::compute::literal(-200));
            } else if (selectivity_ == "10") {
                return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                               arrow::compute::literal(27));
            } else if (selectivity_ == "1") {
                return arrow::compute::greater(arrow::compute::field_ref("total_amount"),
                                               arrow::compute::literal(69));
            }
        }

        arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                                    const arrow::flight::FlightDescriptor& descriptor,
                                    std::unique_ptr<arrow::flight::FlightInfo>* info) {
            ARROW_ASSIGN_OR_RAISE(auto file_info, fs_->GetFileInfo(descriptor.path[0]));
            ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
            *info = std::unique_ptr<arrow::flight::FlightInfo>(
                new arrow::flight::FlightInfo(std::move(flight_info)));
            return arrow::Status::OK();
        }

        arrow::Status Benchmark(const arrow::flight::Ticket& request,
                                         std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
            std::string path;
            ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(request.ticket, &path)); 
            auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
                
            arrow::fs::FileSelector s;
            s.base_dir = std::move(path);
            s.recursive = true;

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

            arrow::dataset::FileSystemFactoryOptions options;
            ARROW_ASSIGN_OR_RAISE(auto factory, 
                arrow::dataset::FileSystemDatasetFactory::Make(std::move(fs), s, std::move(format), options));
            arrow::dataset::FinishOptions finish_options;
            ARROW_ASSIGN_OR_RAISE(auto dataset,factory->Finish(finish_options));

            ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
            ARROW_RETURN_NOT_OK(scanner_builder->Filter(GetFilter()));
            ARROW_RETURN_NOT_OK(scanner_builder->Project(schema->field_names()));

            ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

            if (backend_ == "dataset") {
                std::cout << "Using dataset backend: " << request.ticket << std::endl;
                ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
                *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                    new arrow::flight::RecordBatchStream(reader));
            } else if (backend_ == "dataset+mem") {
                std::cout << "Using dataset+mem backend: " << request.ticket << std::endl;
                ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
                auto im_ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
                ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner_builder, im_ds->NewScan());
                ARROW_ASSIGN_OR_RAISE(auto im_ds_scanner, im_ds_scanner_builder->Finish());
                ARROW_ASSIGN_OR_RAISE(auto reader, im_ds_scanner->ToRecordBatchReader());
                *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                    new arrow::flight::RecordBatchStream(reader));
            }

            return arrow::Status::OK();
        }

        arrow::Status Read(const arrow::flight::Ticket& request,
                           std::unique_ptr<arrow::flight::FlightDataStream>* stream) {

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

            auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

            arrow::dataset::FileSource source;
            if (backend_ == "file") {
                std::cout << "Using file backend: " << request.ticket << std::endl;
                ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(request.ticket));
                source = arrow::dataset::FileSource(file);
            } else if (backend_ == "file+mmap") {
                std::cout << "Using file+mmap backend: " << request.ticket << std::endl;
                ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::MemoryMappedFile::Open(request.ticket, arrow::io::FileMode::READ));
                source = arrow::dataset::FileSource(file);
            }

            ARROW_ASSIGN_OR_RAISE(
                auto fragment, format->MakeFragment(std::move(source), arrow::compute::literal(true)));
            
            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            auto scanner_builder = std::make_shared<arrow::dataset::ScannerBuilder>(
                schema, std::move(fragment), std::move(options));

            ARROW_RETURN_NOT_OK(scanner_builder->Filter(GetFilter()));
            ARROW_RETURN_NOT_OK(scanner_builder->Project(schema->field_names()));

            ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
            ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());

            *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                new arrow::flight::RecordBatchStream(reader));
        
            return arrow::Status::OK();
        }

        arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                            const arrow::flight::Ticket& request,
                            std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
            if (backend_ == "dataset" || backend_ == "dataset+mem") {
                return Benchmark(request, stream);
            } else {
                return Read(request, stream);
            }
        }

    private:
        arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
            const arrow::fs::FileInfo& file_info) {
            std::shared_ptr<arrow::Schema> schema = arrow::schema({});
            std::string path = file_info.path();
            auto descriptor = arrow::flight::FlightDescriptor::Path({path});
            arrow::flight::FlightEndpoint endpoint;
            
            if (backend_ == "dataset" || backend_ == "dataset+mem") {
                endpoint.ticket.ticket = "file://" + path;
            } else {
                endpoint.ticket.ticket = path;
            }

            arrow::flight::Location location;
            ARROW_RETURN_NOT_OK(
                arrow::flight::Location::ForGrpcTcp(host_, port(), &location));
            endpoint.locations.push_back(location);

            return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
        }

        std::shared_ptr<arrow::fs::FileSystem> fs_;
        std::string host_;
        int32_t port_;
        std::string selectivity_;
        std::string backend_;
};

int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cout << "./fs [port] [selectivity] [backend] [transport]" << std::endl;
        exit(1);
    }
    
    std::string host = "10.10.1.2";
    int32_t port = (int32_t)std::stoi(argv[1]);
    std::string selectivity = argv[2]; // 100/10/1
    std::string backend = argv[3]; // file/file+mmap/bake/dataset/dataset+mem
    std::string transport = argv[4]; // tcp+ucx/tcp+grpc

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    arrow::flight::Location server_location;
    if (transport == "tcp+ucx") {
        server_location = arrow::flight::Location::ForScheme("ucx", "[::1]", 0).ValueOrDie();
    } else {
        arrow::flight::Location::ForGrpcTcp(host, port, &server_location);
    }

    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
        new ParquetStorageService(std::move(fs), host, port, selectivity, backend));
    server->Init(options);
    std::cout << "Listening on port " << server->port() << std::endl;
    server->Serve();
}
