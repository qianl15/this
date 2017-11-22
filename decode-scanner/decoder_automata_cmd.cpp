//===================================================//
// File Name: decoder_automata_cmd.cpp
// Author: Qian Li
// Created Date: 2017-11-22
// Description: 
//===================================================//

#include "scanner/video/decoder_automata.h"
#include "scanner/util/fs.h"
#include "scanner/util/opencv.h"  // for using OpenCV

#include <fstream>

extern "C" {
#include "libavcodec/avcodec.h"
}
namespace scanner {
namespace internal {
  
  void decodeFromDisk(int argc, char *argv[]) {
    avcodec_register_all();

    std::fstream decodeArgsFile(argv[1], std::ios::in | std::ios::binary);
    //std::fstream encodedFile(argv[2], std::ios::in | std::ios::binary);
    proto::DecodeArgs loadedDecodeArgs;
    if (!loadedDecodeArgs.ParseFromIstream(&decodeArgsFile)) {
      std::cerr << "Failed to parse address book." << std::endl;
      return;
    }
    std::cout << "The loaded decode args are: \n " << loadedDecodeArgs.DebugString() << std::endl;
    std::vector<proto::DecodeArgs> args;
    /*
    u8* encodedVideoBuffer = new u8[loadedDecodeArgs.encoded_video_size()];
    encodedFile.read(encodedVideoBuffer, loadedDecodeArgs.encoded_video_size());
    */
    
    MemoryPoolConfig config;
    init_memory_allocators(config, {});

    // std::unique_ptr<storehouse::StorageConfig> sc(storehouse::StorageConfig::make_posix_config());

    // auto storage = storehouse::StorageBackend::make_from_config(sc.get());
    VideoDecoderType decoder_type = VideoDecoderType::SOFTWARE;
    DeviceHandle device = CPU_DEVICE;
    DecoderAutomata* decoder = new DecoderAutomata(device, 1, decoder_type);

    // Load test data
    //VideoMetadata video_meta =
    //  read_video_metadata(storage, download_video_meta(short_video));
    std::string videoFileName = argv[2];
    std::vector<u8> video_bytes = read_entire_file(videoFileName);
    u8* video_buffer = new_buffer(CPU_DEVICE, video_bytes.size());
    memcpy_buffer(video_buffer, CPU_DEVICE, video_bytes.data(), CPU_DEVICE,
      video_bytes.size());

    /*
    std::vector<proto::DecodeArgs> args;
    args.emplace_back();
    proto::DecodeArgs& decode_args = args.back();
    decode_args.set_width(video_meta.width());
    decode_args.set_height(video_meta.height());
    decode_args.set_start_keyframe(0);
    decode_args.set_end_keyframe(video_meta.frames());
    for (i64 r = 0; r < video_meta.frames(); ++r) {
      decode_args.add_valid_frames(r);
    }
    for (i64 k : video_meta.keyframe_positions()) {
      decode_args.add_keyframes(k);
    }
    for (i64 k : video_meta.keyframe_byte_offsets()) {
      decode_args.add_keyframe_byte_offsets(k);
    }
    
    decode_args.set_encoded_video_size(video_bytes.size());
    */
    loadedDecodeArgs.set_encoded_video((i64)video_buffer);

    args.push_back(loadedDecodeArgs);
    decoder->initialize(args);


    std::vector<u8> frame_buffer(loadedDecodeArgs.width() * loadedDecodeArgs.height() * 3);

    FrameInfo frame_info(loadedDecodeArgs.height(), loadedDecodeArgs.width(), 
                         3, FrameType::U8);
    std::vector<int> encode_params;
    encode_params.push_back(CV_IMWRITE_JPEG_QUALITY);
    encode_params.push_back(100);
    for (i64 i = 0; i < loadedDecodeArgs.valid_frames().size(); ++i) {
      decoder->get_frames(frame_buffer.data(), 1);
      int ind = loadedDecodeArgs.valid_frames()[i];
      std::string ind_str = std::to_string(ind);
      
      // printf("for frame %li the red component of a pixel has value: %i \n", i, frame_buffer[300000]);
      // printf("for frame %li the green component of the a pixel has value: %i \n", i, frame_buffer[300001]);
      // printf("for frame %li the blue component of the a pixel has value: %i \n", i, frame_buffer[300002]);

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
      std::fstream output_buff("frame" + ind_str + ".jpg",
          std::ios::out | std::ios::trunc | std::ios::binary);
      output_buff.write(str_encode.c_str(), str_encode.size());
      printf("Save frame%d.jpg to disk \n", ind);
      delete frame;
    }

    
    delete decoder;
    // delete storage;
    destroy_memory_allocators();
  }
}
}

int main(int argc, char *argv[]) {
  scanner::internal::decodeFromDisk(argc, argv);
  return 0;
}