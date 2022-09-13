#include <iostream>
#include <time.h>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

class MeasureExecutionTime{
  private:
    const std::chrono::steady_clock::time_point begin;
    const std::string caller;
  public:
    MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()){}
    ~MeasureExecutionTime(){
        const auto duration=std::chrono::steady_clock::now()-begin;
        std::cout << (double)std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()/1000 << " seconds" <<std::endl;
    }
};

#ifndef MEASURE_FUNCTION_EXECUTION_TIME
#define MEASURE_FUNCTION_EXECUTION_TIME const MeasureExecutionTime measureExecutionTime(__FUNCTION__);
#endif

struct ConnectionInfo {
  std::string host;
  int32_t port;
};

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> ConnectToFlightServer(ConnectionInfo info) {
  arrow::flight::Location location;
  ARROW_RETURN_NOT_OK(
    arrow::flight::Location::ForGrpcTcp(info.host, info.port, &location));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
  return client;
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cout << "./fc [port] [backend]" << std::endl;
    exit(1);
  }

  ConnectionInfo info;
  std::string host = "10.10.1.2";
  info.host = host;
  info.port = (int32_t)std::stoi(argv[1]);
  std::string backend = argv[2]; // dataset/file

  auto client = ConnectToFlightServer(info).ValueOrDie();

  if (backend == "dataset") {
    std::string filepath = "/mnt/cephfs/dataset";
    auto descriptor = arrow::flight::FlightDescriptor::Path({filepath});
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    client->GetFlightInfo(descriptor, &flight_info);
    std::shared_ptr<arrow::Table> table;
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    client->DoGet(flight_info->endpoints()[0].ticket, &stream);
    {
      MEASURE_FUNCTION_EXECUTION_TIME
      stream->ReadAll(&table);
    }
    std::cout << "Read " << table->num_rows() << " rows and " << table->num_columns() << " columns" << std::endl;

  } else {  
    int64_t total_rows = 0;
    {
      MEASURE_FUNCTION_EXECUTION_TIME
      for (int i = 1; i <= 200; i++) {
        std::string filepath = "/mnt/cephfs/dataset/16MB.uncompressed.parquet." + std::to_string(i);
        auto descriptor = arrow::flight::FlightDescriptor::Path({filepath});

        std::unique_ptr<arrow::flight::FlightInfo> flight_info;
        client->GetFlightInfo(descriptor, &flight_info);

        std::shared_ptr<arrow::Table> table;
        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        client->DoGet(flight_info->endpoints()[0].ticket, &stream);
        stream->ReadAll(&table);
        total_rows += table->num_rows();
      }
    }
    std::cout << "Read " << total_rows << " rows" << std::endl;
  }
}
