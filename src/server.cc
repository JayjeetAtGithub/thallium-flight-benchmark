// #include <iostream>

// #include "arrow/api.h"
// #include "arrow/compute/exec/expression.h"
// #include "arrow/dataset/api.h"
// #include "arrow/filesystem/api.h"
// #include "arrow/io/api.h"
// #include "arrow/util/checked_cast.h"
// #include "arrow/util/iterator.h"
// #include "parquet/arrow/reader.h"
// #include "parquet/arrow/writer.h"
// #include <thallium.hpp>


// arrow::Result<std::shared_ptr<arrow::Table>> Scan() {
//     std::shared_ptr<arrow::fs::LocalFileSystem> fs =
//         std::make_shared<arrow::fs::LocalFileSystem>();

//     arrow::fs::FileSelector selector;
//     selector.base_dir = "/mnt/cephfs/dataset";
//     selector.recursive = true;

//     ARROW_ASSIGN_OR_RAISE(std::vector<arrow::fs::FileInfo> file_infos,
//         fs->GetFileInfo(selector));

//     std::shared_ptr<arrow::dataset::ParquetFileFormat> format =
//         std::make_shared<arrow::dataset::ParquetFileFormat>();

//     arrow::dataset::FileSystemFactoryOptions options;
//     ARROW_ASSIGN_OR_RAISE(
//         std::shared_ptr<arrow::dataset::DatasetFactory> dataset_factory,
//         arrow::dataset::FileSystemDatasetFactory::Make(fs, selector, format, options));

//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset,
//         dataset_factory->Finish());

//     arrow::dataset::ScannerBuilder scanner_builder(dataset);
//     ARROW_RETURN_NOT_OK(scanner_builder.UseThreads(true));
//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Scanner> scanner,
//                         scanner_builder.Finish());

//     ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, scanner->ToTable());
//     return table;
// }

// void hello(const thallium::request& req) {
//     std::shared_ptr<arrow::Table> table = Scan().ValueOrDie();
//     std::cout << table->num_rows() << std::endl;
// }

// int main(int argc, char** argv) {

//     thallium::engine engine("tcp", THALLIUM_SERVER_MODE);
//     engine.define("hello", hello).disable_response();
//     std::cout << "Server running at address " << engine.self() << std::endl;

//     return 0;
// }


#include <iostream>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

int main(int argc, char** argv) {

    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    std::cout << "Server running at address " << engine.self() << std::endl;
    
    std::string buffer = "Matthieu";
    std::vector<std::pair<void*,std::size_t>> segments(1);
    segments[0].first  = (void*)(&buffer[0]);
    segments[0].second = buffer.size()+1;

    tl::bulk myBulk = engine.expose(segments, tl::bulk_mode::read_only);

    remote_do_rdma.on(server_endpoint)(myBulk);
}