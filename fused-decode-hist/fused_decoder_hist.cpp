//===================================================//
// File Name: fused_decoder_hist.cpp
// Author: Qian Li
// Created Date: 2017-12-3
// Description: A fused decoder + hist kernel
// We don't have to write the output jpgs!
//===================================================//

#include "scanner/video/decoder_automata.h"
#include "scanner/util/fs.h"
#include "scanner/util/opencv.h"  // for using OpenCV

#include <fstream>
#include <sys/stat.h>

extern "C" {
#include "libavcodec/avcodec.h"
}
namespace scanner {
namespace internal {
namespace {
const i32 BINS = 16;
}  
  void checkOutputDir(std::string &dir) {
    struct stat st;
    if (stat(dir.c_str(), &st) == 0) {
      if (st.st_mode & S_IFDIR != 0) {
        printf("Output dir: %s exists!\n", dir.c_str());
      }
    } else {
      printf("Creating dir: %s ...\n", dir.c_str());
      const int dir_err = mkdir(dir.c_str(),  S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
      if (dir_err != 0) {
        printf("Error creating dir: %s !\n", dir.c_str());
        exit(-1);
      }
    }
  }

  size_t execute_hist(const cv::Mat &img, std::ofstream &output_buff, 
                    int currFrame) {
    float range[] = {0, 256};
    const float* histRange = {range};
    size_t hist_size = BINS * 3 * sizeof(int);
    u8 *output_buf = new_buffer(CPU_DEVICE, hist_size);
    for (i32 j = 0; j < 3; ++j) {
      int channels[] = {j};
      cv::Mat hist;
      cv::calcHist(&img, 1, channels, cv::Mat(),
                   hist,
                   1, &BINS,
                   &histRange);
      cv::Mat out(BINS, 1, CV_32SC1, output_buf + j * BINS * sizeof(int));
      hist.convertTo(out, CV_32SC1);
    }
    output_buff.write((char *)output_buf, hist_size);
    if (output_buff.fail()) {
      printf("Failed to save frame%d hist to disk, size %lu KB\n",
        currFrame, hist_size / 1024);
      exit(-1);
    }
    return hist_size;
  }

  void decodeFromDisk(int argc, char *argv[]) {
    avcodec_register_all();
    std::string output_dir = "/tmp/output";
    if (argc >= 4) {
      output_dir = argv[3];
    }
    
    checkOutputDir(output_dir);

    std::fstream decodeArgsFile(argv[1], std::ios::in | std::ios::binary);

    proto::DecodeArgs loadedDecodeArgs;
    if (!loadedDecodeArgs.ParseFromIstream(&decodeArgsFile)) {
      std::cerr << "Failed to parse address book." << std::endl;
      return;
    }

    std::vector<proto::DecodeArgs> args;
    
    MemoryPoolConfig config;
    init_memory_allocators(config, {});

    VideoDecoderType decoder_type = VideoDecoderType::SOFTWARE;
    DeviceHandle device = CPU_DEVICE;
    DecoderAutomata* decoder = new DecoderAutomata(device, 1, decoder_type);

    // Load test data
    std::string videoFileName = argv[2];
    std::vector<u8> video_bytes = read_entire_file(videoFileName);
    u8* video_buffer = new_buffer(CPU_DEVICE, video_bytes.size());
    memcpy_buffer(video_buffer, CPU_DEVICE, video_bytes.data(), CPU_DEVICE,
      video_bytes.size());

    loadedDecodeArgs.set_encoded_video((i64)video_buffer);

    args.push_back(loadedDecodeArgs);
    decoder->initialize(args);


    std::vector<u8> frame_buffer(loadedDecodeArgs.width() * loadedDecodeArgs.height() * 3);

    FrameInfo frame_info(loadedDecodeArgs.height(), loadedDecodeArgs.width(), 
                         3, FrameType::U8);
    std::vector<int> encode_params;
    encode_params.push_back(CV_IMWRITE_JPEG_QUALITY);
    encode_params.push_back(100);
    size_t total_size = 0;
    int first_frame = -1;

    int ind = loadedDecodeArgs.valid_frames()[0];
    char outFileName[100];
    sprintf(outFileName, "%s/hist%d-%lu.out", output_dir.c_str(), ind, loadedDecodeArgs.valid_frames().size());
    std::ofstream output_buff(outFileName,
      std::ios::out | std::ios::trunc | std::ios::binary);
    for (i64 i = 0; i < loadedDecodeArgs.valid_frames().size(); ++i) {
      decoder->get_frames(frame_buffer.data(), 1);
      int ind = loadedDecodeArgs.valid_frames()[i];
      const scanner::Frame* frame = new Frame(frame_info, frame_buffer.data());
      cv::Mat img = scanner::frame_to_mat(frame);

      auto hist_size = execute_hist(img, output_buff, ind);

      total_size += hist_size;
      delete frame;
    }

    printf("Save %lu KB to %s\n", total_size / 1024, outFileName);
    delete decoder;
    destroy_memory_allocators();
  }
}
}

int main(int argc, char *argv[]) {
  if (argc < 3 || argc > 4) {
    std::cout << "Usage: FusedDecoderHist <proto_file> <bin_file> (<output_dir>) \n";
    std::cout << "Example:\n";
    std::cout << "decode_args1000.proto start_frame1000.bin /tmp/output\n";
    exit(-1);
  }
  scanner::internal::decodeFromDisk(argc, argv);
  return 0;
}