//===================================================//
// File Name: decoder_automata_cmd.cpp
// Author: Qian Li
// Created Date: 2017-11-22
// Description: 
//===================================================//

#include "scanner/video/decoder_automata.h"
#include "scanner/util/fs.h"
#include "scanner/util/cuda.h"
#include "scanner/util/opencv.h"  // for using OpenCV
//#include "scanner/video/nvidia/nvidia_video_decoder.h"
#include "scanner/video/video_decoder.h"
#include <fstream>
#include <sys/stat.h>

extern "C" {
#include "libavcodec/avcodec.h"
}
namespace scanner {
namespace internal {
  
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
    // std::cout << "The loaded decode args are: \n " << loadedDecodeArgs.DebugString() << std::endl;
    std::vector<proto::DecodeArgs> args;
    
    MemoryPoolConfig config;
    init_memory_allocators(config, {0});
//    std::cout << cudaGetErrorString(1) << std::endl;
#ifdef HAVE_CUDA
    std::cout << "HAVE_CUDA is " <<std::endl;
#else
    std::cout << "don't HAVE_CUDA" << std::endl;
#endif
    CUDA_PROTECT(printf("hi\n"));
    int x = -10;
    int res = VideoDecoder::has_decoder_type(VideoDecoderType::NVIDIA); 
    if (res){ 
        std::cout << "have nvidia decoder" << std::endl;
    }
    //cudaGetDeviceCount(&x);
    printf("device count: %i\n", x);

    VideoDecoderType decoder_type = VideoDecoderType::NVIDIA;
    printf("hi2\n");
    DeviceHandle GPU_DEVICE = {DeviceType::GPU, 0};
    printf("hi3\n");
    //DeviceHandle device = GPU_DEVICE;
    DecoderAutomata* decoder = new DecoderAutomata(GPU_DEVICE, 1, decoder_type);
    printf("hi4\n");

    // Load test data
    std::string videoFileName = argv[2];
    std::vector<u8> video_bytes = read_entire_file(videoFileName);
    printf("hi4.5\n");
    u8* video_buffer = new_buffer(GPU_DEVICE, video_bytes.size());
    printf("hi5\n");
    memcpy_buffer(video_buffer, GPU_DEVICE, video_bytes.data(), CPU_DEVICE,
      video_bytes.size());

    printf("hi6\n");
    loadedDecodeArgs.set_encoded_video((i64)video_buffer);

    args.push_back(loadedDecodeArgs);
    decoder->initialize(args);

    u8* frame_buffer_gpu = new_buffer(GPU_DEVICE, loadedDecodeArgs.width() * loadedDecodeArgs.height() * 3);
    std::vector<u8> frame_buffer(loadedDecodeArgs.width() * loadedDecodeArgs.height() * 3);

    FrameInfo frame_info(loadedDecodeArgs.height(), loadedDecodeArgs.width(), 
                         3, FrameType::U8);
    std::vector<int> encode_params;
    encode_params.push_back(CV_IMWRITE_JPEG_QUALITY);
    encode_params.push_back(100);
    size_t total_size = 0;
    int first_frame = -1;
    for (i64 i = 0; i < loadedDecodeArgs.valid_frames().size(); ++i) {
      printf("hi7\n");
      decoder->get_frames(frame_buffer_gpu, 1);
      printf("hi7.5\n");
      memcpy_buffer(frame_buffer.data(), CPU_DEVICE, frame_buffer_gpu, GPU_DEVICE, frame_buffer.size());
      //decoder->get_frames(frame_buffer.data(), 1);
      printf("hi8\n");
      int ind = loadedDecodeArgs.valid_frames()[i];
      std::string ind_str = std::to_string(ind);
      if (first_frame == -1) {
        first_frame = ind;
      }
      const scanner::Frame* frame = new Frame(frame_info, frame_buffer.data());
      cv::Mat img = scanner::frame_to_mat(frame);
      std::vector<u8> buf;
      cv::Mat recolored;
      cv::cvtColor(img, recolored, CV_RGB2BGR);
      bool success = cv::imencode(".jpg", recolored, buf, encode_params);
      if(!success) {
        std::cout << "Failed to encode image" << std::endl;
        exit(1);
      }
      std::string str_encode(buf.begin(), buf.end());
      std::ofstream output_buff(output_dir + "/frame" + ind_str + ".jpg",
          std::ios::out | std::ios::trunc | std::ios::binary);
      output_buff.write(str_encode.c_str(), str_encode.size());
      if (output_buff.fail()) {
        printf("Failed to save frame%d.jpg to disk, size %lu KB\n",
          ind, str_encode.size() / 1024);
        exit(-1);
      }
      // printf("Save frame%d.jpg to disk, size %lu KB\n", 
      //         ind, str_encode.size() / 1024);
      total_size += str_encode.size();
      delete frame;
    }

    printf("Decoder generated # %lu files from frame %d. Total size is: %lu MB\n",
           loadedDecodeArgs.valid_frames().size(), 
           first_frame,
           total_size / (1024 * 1024));
    delete decoder;
    // delete storage;
    destroy_memory_allocators();
  }
}
}

int main(int argc, char *argv[]) {
  if (argc < 3 || argc > 4) {
    std::cout << "Usage: DecoderAutomataCmd <proto_file> <bin_file> (<output_dir>) \n";
    std::cout << "Example:\n";
    std::cout << "decode_args1000.proto start_frame1000.bin /tmp/output\n";
    exit(-1);
  }
  scanner::internal::decodeFromDisk(argc, argv);
  return 0;
}
