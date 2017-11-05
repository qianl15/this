//===================================================//
// File Name: mxnetlambda_op.cpp
// Author: Qian Li
// Created Date: 2017-11-04
// Description: Call MXNet via AWS lambda, we registered
// this as a kernel in Scanner
//===================================================//

#include "mxnetlambda.pb.h"            // for ResizeArgs (generated file)
#include "scanner/api/kernel.h"   // for VideoKernel and REGISTER_KERNEL
#include "scanner/api/op.h"       // for REGISTER_OP
#include "scanner/util/memory.h"  // for device-independent memory management
#include "scanner/util/opencv.h"  // for using OpenCV
#include "scanner/util/common.h"
#include <string>
#include <linux/types.h>
#include <iostream>
#include <istream>
#include <ostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/asio/ssl.hpp>
#include <fstream>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/archive/iterators/insert_linebreaks.hpp>
#include "json.hpp"

using u8 = uint8_t;
using namespace boost::archive::iterators;
using boost::asio::ip::tcp;
using json = nlohmann::json;

class client
{
public:
    std::stringstream resultss;
    client(boost::asio::io_service& io_service,
           const std::string& server, const std::string& path, const std::string& binaryImgStr)
            : resolver_(io_service),
              ctx(io_service, boost::asio::ssl::context::method::tlsv12_client),
              socket_(io_service, ctx)
    {
        SSL_set_tlsext_host_name(socket_.native_handle(),server.c_str());
        ctx.set_default_verify_paths();
        
        std::string b64ImgStr = base64_encode(reinterpret_cast<const unsigned char*>(binaryImgStr.c_str()), binaryImgStr.length());
        // Form the request. We specify the "Connection: close" header so that the
        // server will close the socket after transmitting the response. This will
        // allow us to treat all data up until the EOF as the content.
        std::ostream request_stream(&request_);
        request_stream << "POST " << path << " HTTP/1.1\r\n";
        request_stream << "Host: " << server << "\r\n";
        request_stream << "content-type: application/json;charset=UTF-8\r\n";
        std::string bodyStartStr = "{\"httpMethod\":\"POST\",\"pathWithQueryString\":\"/mxnet-test-dev-hello\",\"body\":\"{\\\"b64Img\\\": \\\"";
        std::string bodyEndStr = "\\\"}\",\"headers\":{},\"stageVariables\":{},\"withAuthorization\":false}\r\n";
        request_stream << "content-length: " << std::to_string(bodyStartStr.length() + b64ImgStr.length() + bodyEndStr.length()) << "\r\n";
        request_stream << "Connection: close\r\n\r\n";
        request_stream << bodyStartStr << b64ImgStr << bodyEndStr;

        // Start an asynchronous resolve to translate the server and service names
        // into a list of endpoints.
        tcp::resolver::query query(server, "https");

        resolver_.async_resolve(query,
                                boost::bind(&client::handle_resolve, this,
                                            boost::asio::placeholders::error,
                                            boost::asio::placeholders::iterator));
    }

    ~client() {
       
    }
private:


    // http://renenyffenegger.ch/notes/development/Base64/Encoding-and-decoding-base-64-with-cpp
    const std::string base64_chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "abcdefghijklmnopqrstuvwxyz"
                    "0123456789+/";

    std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
        std::string ret;
        int i = 0;
        int j = 0;
        unsigned char char_array_3[3];
        unsigned char char_array_4[4];

        while (in_len--) {
            char_array_3[i++] = *(bytes_to_encode++);
            if (i == 3) {
                char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
                char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
                char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
                char_array_4[3] = char_array_3[2] & 0x3f;

                for(i = 0; (i <4) ; i++)
                    ret += base64_chars[char_array_4[i]];
                i = 0;
            }
        }

        if (i)
        {
            for(j = i; j < 3; j++)
                char_array_3[j] = '\0';

            char_array_4[0] = ( char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

            for (j = 0; (j < i + 1); j++)
                ret += base64_chars[char_array_4[j]];

            while((i++ < 3))
                ret += '=';

        }

        return ret;

    }

    void handle_resolve(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator)
    {
        if (!err)
        {
            // std::cout << "Resolve OK" << "\n";
            socket_.set_verify_mode(boost::asio::ssl::verify_peer);
            socket_.set_verify_callback(
                    boost::bind(&client::verify_certificate, this, _1, _2));

            boost::asio::async_connect(socket_.lowest_layer(), endpoint_iterator,
                                       boost::bind(&client::handle_connect, this,
                                                   boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Error resolve: " << err.message() << "\n";
        }
    }

    bool verify_certificate(bool preverified,
                            boost::asio::ssl::verify_context& ctx)
    {
        // The verify callback can be used to check whether the certificate that is
        // being presented is valid for the peer. For example, RFC 2818 describes
        // the steps involved in doing this for HTTPS. Consult the OpenSSL
        // documentation for more details. Note that the callback is called once
        // for each certificate in the certificate chain, starting from the root
        // certificate authority.

        // In this example we will simply print the certificate's subject name.
        char subject_name[256];
        X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
        X509_NAME_oneline(X509_get_subject_name(cert), subject_name, 256);
        // std::cout << "Verifying " << subject_name << "\n";

        return preverified;
    }

    void handle_connect(const boost::system::error_code& err)
    {
        if (!err)
        {
            // std::cout << "Connect OK " << "\n";
            // socket_.lowest_layer().set_option(tcp::no_delay(true));
            socket_.async_handshake(boost::asio::ssl::stream_base::client,
                                    boost::bind(&client::handle_handshake, this,
                                                boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Connect failed: " << err.message() << "\n";
        }
    }
  
    void handle_handshake(const boost::system::error_code& error)
    {
        if (!error)
        {
            // std::cout << "Handshake OK " << "\n";
            // std::cout << "Request: " << "\n";
            // const char* header=boost::asio::buffer_cast<const char*>(request_.data());
            // std::cout << header << "\n";

            // The handshake was successful. Send the request.
            boost::asio::async_write(socket_, request_,
                                     boost::bind(&client::handle_write_request, this,
                                                 boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Handshake failed: " << error.message() << "\n";
        }
    }

    void handle_write_request(const boost::system::error_code& err)
    {
        if (!err)
        {
            // Read the response status line. The response_ streambuf will
            // automatically grow to accommodate the entire line. The growth may be
            // limited by passing a maximum size to the streambuf constructor.
            // std::cout << "finished transfer data" << std::endl;
            boost::asio::async_read_until(socket_, response_, "\r\n",
                                          boost::bind(&client::handle_read_status_line, this,
                                                      boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Error write req: " << err.message() << "\n";
        }
    }

    void handle_read_status_line(const boost::system::error_code& err)
    {
        if (!err)
        {
            // Check that response is OK.
            std::istream response_stream(&response_);
            std::string http_version;
            response_stream >> http_version;
            unsigned int status_code;
            response_stream >> status_code;
            std::string status_message;
            std::getline(response_stream, status_message);
            if (!response_stream || http_version.substr(0, 5) != "HTTP/")
            {
                std::cout << "Invalid response\n";
                return;
            }
            if (status_code != 200)
            {
                std::cout << "Response returned with status code ";
                std::cout << status_code << "\n";
                return;
            }
            // std::cout << "Status code: " << status_code << "\n";

            // Read the response headers, which are terminated by a blank line.
            boost::asio::async_read_until(socket_, response_, "\r\n\r\n",
                                          boost::bind(&client::handle_read_headers, this,
                                                      boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Error: " << err.message() << "\n";
        }
    }

    void handle_read_headers(const boost::system::error_code& err)
    {
        if (!err)
        {
            // Process the response headers.
            std::istream response_stream(&response_);
            std::string header;
            while (std::getline(response_stream, header) && header != "\r") {
                // std::cout << header << "\n";
            }
            // std::cout << "\n";

            // Write whatever content we already have to output.
            if (response_.size() > 0)
                resultss << &response_;

            // Start reading remaining data until EOF.
            boost::asio::async_read(socket_, response_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&client::handle_read_content, this,
                                                boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Error: " << err << "\n";
        }
    }

    void handle_read_content(const boost::system::error_code& err)
    {
        if (!err)
        {
            // std::cout << "reading the content..." << std::endl;
            // Write all of the data that has been read so far.
            // std::cout << &response_;
            resultss << &response_;
            // Continue reading remaining data until EOF.
            boost::asio::async_read(socket_, response_,
                                    boost::asio::transfer_at_least(1),
                                    boost::bind(&client::handle_read_content, this,
                                                boost::asio::placeholders::error));
        }
        else if (err != boost::asio::error::eof)
        {
            std::cout << "Error: " << err << "\n";
        } 
    }

    tcp::resolver resolver_;
    // https://stackoverflow.com/questions/40036854/creating-a-https-request-using-boost-asio-and-openssl
    // http://www.boost.org/doc/libs/1_65_1/doc/html/boost_asio/overview/ssl.html
    boost::asio::ssl::context ctx;
    boost::asio::ssl::stream<tcp::socket> socket_;
    boost::asio::streambuf request_;
    boost::asio::streambuf response_;
};

std::string parse_result(const std::string& resultstr) {

    json js = json::parse(resultstr);
    std::string bodystr = js["body"];
    // std::cout << "\n The body content is: \n";
    // std::cout << bodystr << std::endl;

    json bodyjs = json::parse(bodystr);
    // std::cout << "\nHighest possible class is: ";
    // std::cout << bodyjs["0"] << "\n";
    json objs = bodyjs["0"];
    std::string ret_class = objs.begin().key();
    // std::cout << ret_class << "\n";
    return ret_class;
}

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

    std::vector<int> encode_params;
    encode_params.push_back(CV_IMWRITE_JPEG_QUALITY);
    encode_params.push_back(100);
    int input_count = num_rows(frame_column);

    size_t class_size = sizeof(int);
    u8* output_block =
        scanner::new_block_buffer(device_, class_size * input_count, input_count);


    std::vector<client *> lambda_frames;
    boost::asio::io_service io_service;
    std::vector<std::string> str_encodes;

    for (int i = 0; i < input_count; ++i) {
      // Get a frame from the batch of input frames

      // First, do frame encoding, transform to jpg
      const scanner::Frame* frame = frame_column[i].as_const_frame();
      cv::Mat img = scanner::frame_to_mat(frame);
      std::vector<u8> buf;
      cv::Mat recolored;
      cv::cvtColor(img, recolored, CV_RGB2BGR);
      bool success = cv::imencode(".jpg", recolored, buf, encode_params);
      if(!success) {
        std::cout << "Failed to encode image" << std::endl;
        exit(1);
      }
      // size_t orig_size = img.total() * img.elemSize();
      // printf("compression: origin: %lu, jpg: %lu, rate: %lf\n", 
      //   orig_size, buf.size(), (double)buf.size() / orig_size);

      // convert to binary string!
      std::string str_encode(buf.begin(), buf.end()); 
      str_encodes.push_back(str_encode);

      // Start to asynchronously launch lambdas!
      client *pc;
      pc = new client(io_service, server_, path_, str_encode);
      io_service.poll(); // run the ready handlers.
      lambda_frames.push_back(pc);
      std::cout << "launch lambda: " << i << std::endl;
    }

    io_service.run();
    io_service.reset();

    for (int i = 0; i < input_count; ++i) {
      std::cout << "lambda " << i << " finished" << std::endl;
      int result_class;
      std::string result;
        if (lambda_frames[i]->resultss.str().size() > 0) {
            result = parse_result(lambda_frames[i]->resultss.str());
            result_class = std::stoi(result);
        }
        else {
            std::cout << "error happened, retry" << std::endl;
            // err_cnt++;
            delete lambda_frames[i];
            lambda_frames[i] = NULL;
            client *pc = new client(io_service, server_, path_, str_encodes[i]);
            io_service.run();
            io_service.reset();
            std::cout << "retry finished" << std::endl;
            if (pc->resultss.str().size() > 0) {
                result = parse_result(pc->resultss.str());
                result_class = std::stoi(result);
            } else {
                result_class = 0;
            }
            delete pc;
            pc = NULL;
        }

      u8* output_buf = output_block + i * class_size;
      // *((int *)output_buf) = i;
      std::memcpy(output_buf, &result_class, class_size);
      // printf("%d\n", *((int *)output_buf));
      scanner::insert_element(output_columns[0], output_buf, class_size);
      if (lambda_frames[i] != NULL) {
        delete lambda_frames[i];
      }
      lambda_frames[i] = NULL;
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
