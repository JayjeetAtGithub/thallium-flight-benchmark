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
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>     

#include <iostream>
#include <memory>
#include <utility>
#include <vector>


namespace fs = arrow::fs;
namespace ds = arrow::dataset;
namespace cp = arrow::compute;


using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;


std::shared_ptr<arrow::DataType> type_from_id(int type_id) {
    std::shared_ptr<arrow::DataType> type;
    switch(type_id) {
        case 1:
            type = arrow::boolean();
            break;
        case 9:
            type = arrow::int64();
            break;
        case 12:
            type = arrow::float64();
            break;
        case 13:
            type = arrow::binary();
            break;
        default:
            std::cout << "Unknown type" << std::endl;
            exit(1);
    }
    return type;     
}

std::string generate_uuid() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return boost::uuids::to_string(uuid);
}

std::shared_ptr<fs::FileSystem> GetFileSystemFromUri(const std::string& uri,
                                                     std::string* path) {
  return fs::FileSystemFromUri(uri, path).ValueOrDie();
}

std::shared_ptr<ds::Dataset> GetDatasetFromDirectory(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string dir) {
  fs::FileSelector s;
  s.base_dir = dir;
  s.recursive = true;

  ds::FileSystemFactoryOptions options;
  auto factory = ds::FileSystemDatasetFactory::Make(fs, s, format, options).ValueOrDie();
  auto schema = factory->Inspect().ValueOrDie();
  auto child = factory->Finish().ValueOrDie();
  ds::DatasetVector children{1, child};
  auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));
  return dataset.ValueOrDie();
}

std::shared_ptr<ds::Dataset> GetParquetDatasetFromMetadata(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string metadata_path) {
  ds::ParquetFactoryOptions options;
  auto factory =
      ds::ParquetDatasetFactory::Make(metadata_path, fs, format, options).ValueOrDie();
  return factory->Finish().ValueOrDie();
}

std::shared_ptr<ds::Dataset> GetDatasetFromFile(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string file) {
  ds::FileSystemFactoryOptions options;
  auto factory =
      ds::FileSystemDatasetFactory::Make(fs, {file}, format, options).ValueOrDie();
  auto schema = factory->Inspect().ValueOrDie();
  auto child = factory->Finish().ValueOrDie();
  ds::DatasetVector children;
  children.resize(1, child);
  auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));
  return dataset.ValueOrDie();
}

std::shared_ptr<ds::Dataset> GetDatasetFromPath(
    std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
    std::string path) {
  auto info = fs->GetFileInfo(path).ValueOrDie();
  if (info.IsDirectory()) {
    return GetDatasetFromDirectory(fs, format, path);
  }

  auto dirname_basename = arrow::fs::internal::GetAbstractPathParent(path);
  auto basename = dirname_basename.second;

  if (basename == "_metadata") {
    return GetParquetDatasetFromMetadata(fs, format, path);
  }

  return GetDatasetFromFile(fs, format, path);
}

arrow::Result<std::shared_ptr<ds::Dataset>> GetDataset() {
  auto format = std::make_shared<ds::ParquetFileFormat>();
  std::string path;
  auto fs = GetFileSystemFromUri("file:///mnt/cephfs/dataset", &path);
  auto dataset = GetDatasetFromPath(fs, format, path); 
  return dataset;
}
