#include <memory>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/logging.h>

#include <bake-client.hpp>
#include <bake-server.hpp>


static char* read_input_file(const char* filename);

namespace bk = bake;

class random_access_object : public arrow::io::RandomAccessFile {
 public:
  explicit random_access_object(void *ptr, uint64_t size) {
    file_ptr = ptr;
    file_size = size;
  }

  ~random_access_object() override { DCHECK_OK(Close()); }

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
        "ReadAt has not been implemented in random_access_object");
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, file_size - position);

    if (nbytes > 0) {
      return std::make_shared<arrow::Buffer>((uint8_t*)file_ptr + position, nbytes);
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
  void *file_ptr = NULL;
  int64_t file_size = -1;
};


int main(int argc, char* argv[]) {    
    margo_instance_id mid = margo_init("verbs://ibp130s0", MARGO_SERVER_MODE, 0, 0);
    if (mid == MARGO_INSTANCE_NULL) {
        std::cerr << "Error: margo_init()\n";
        return -1;
    }
    
    hg_addr_t svr_addr;
    hg_return_t hret = margo_addr_self(mid, &svr_addr);
    if (hret != HG_SUCCESS) {
        std::cerr << "Error: margo_addr_lookup()\n";
        margo_finalize(mid);
        return -1;
    }

    char *config = read_input_file("bake/config.json");

    // setup provider
    bk::provider *p = bk::provider::create(
        mid, 0, ABT_POOL_NULL, std::string(config, strlen(config) + 1), ABT_IO_INSTANCE_NULL, NULL, NULL);

    std::cout << "successfully setup provider" << std::endl;
    std::string cfg = p->get_config();
    std::cout << cfg << std::endl;

    bk::client bcl(mid);
    bk::provider_handle bph(bcl, svr_addr, 0);
    bph.set_eager_limit(0);
    bk::target tid = p->list_targets()[0];

    // try zero copy access
    bake::region rid("AAAAAO0B3hifXASeUAk8AAAAAAA=");
    char* zero_copy_pointer = (char*)bcl.get_data(bph, tid, rid);
    std::string str((char*)zero_copy_pointer, 3);
    std::cout << str << std::endl;

    // free resources
    margo_addr_free(mid, svr_addr);
    margo_finalize(mid);
    return 0;
}

static char* read_input_file(const char* filename)
{
    size_t ret;
    FILE*  fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Could not open %s\n", filename);
        exit(-1);
    }
    fseek(fp, 0, SEEK_END);
    size_t sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char* buf = (char*)calloc(1, sz + 1);
    ret       = fread(buf, 1, sz, fp);
    if (ret != sz && ferror(fp)) {
        free(buf);
        perror("read_input_file");
        buf = NULL;
    }
    fclose(fp);
    return buf;
}