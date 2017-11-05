#include "mxnetlambda.pb.h"            // for ResizeArgs (generated file)
#include "scanner/api/kernel.h"   // for VideoKernel and REGISTER_KERNEL
#include "scanner/api/op.h"       // for REGISTER_OP
#include "scanner/util/memory.h"  // for device-independent memory management
#include "scanner/util/opencv.h"  // for using OpenCV
#include "scanner/util/common.h"
#include <string>
#include <linux/types.h>
using u8 = uint8_t;

class MxnetLambdaKernel : public scanner::BatchedKernel {
 public:
 
  MxnetLambdaKernel(const scanner::KernelConfig& config)
      : scanner::BatchedKernel(config), device_(config.devices[0]) {
    // The protobuf arguments must be decoded from the input string.
    MxnetLambdaArgs args;
    args.ParseFromArray(config.args.data(), config.args.size());
    server_ = args.server();
    path_ = args.path();
  }

  // Execute is the core computation of the kernel. It maps a batch of rows
  // from an input table to a batch of rows of the output table. Here, we map
  // from one input column from the video, "frame", and return
  // a single column, "class" (from ImageNet synset.txt).
  void execute(const scanner::BatchedColumns& input_columns,
               scanner::BatchedColumns& output_columns) override {
    auto& frame_column = input_columns[0];
    int input_count = num_rows(frame_column);

    // This must be called at the top of the execute method in any VideoKernel.
    // See the VideoKernel for the implementation check_frame_info.
    // scanner::VideoKernel::check_frame(scanner::CPU_DEVICE, frame_column[0]);
    size_t class_size = sizeof(int);
    // printf("class size: %lu\n", class_size);
    u8* output_block =
        scanner::new_block_buffer(device_, class_size * input_count, input_count);

    for (int i = 0; i < input_count; ++i) {
      // Get a frame from the batch of input frames
      const scanner::Frame* frame = frame_column[i].as_const_frame();
      cv::Mat input = scanner::frame_to_mat(frame);

      // Allocate a frame for the resized output frame
      // scanner::Frame* resized_frame =
      //   scanner::new_frame(scanner::CPU_DEVICE, output_frame_info);
      // cv::Mat output = scanner::frame_to_mat(resized_frame);

      // cv::resize(input, output, cv::Size(width_, height_));
      u8* output_buf = output_block + i * class_size;
      // *((int *)output_buf) = i;
      std::memcpy(output_buf, &i, class_size);
      // printf("%d\n", *((int *)output_buf));
      scanner::insert_element(output_columns[0], output_buf, class_size);
    }
  }

 private:
  std::string server_;
  std::string path_;
  scanner::DeviceHandle device_;
};

// These functions run statically when the shared library is loaded to tell the
// Scanner runtime about your custom op.

REGISTER_OP(MxnetLambda).frame_input("frame").output("class");

REGISTER_KERNEL(MxnetLambda, MxnetLambdaKernel)
    .device(scanner::DeviceType::CPU)
    .batch()
    .num_devices(1);
