//
// async_client.cpp
// ~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2010 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

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

using namespace boost::archive::iterators;
using boost::asio::ip::tcp;

class client
{
public:
    client(boost::asio::io_service& io_service,
           const std::string& server, const std::string& path, const std::string& binaryImgStr)
            : resolver_(io_service),
              ctx(io_service, boost::asio::ssl::context::method::tlsv12_client),
              socket_(io_service, ctx)
    {
        SSL_set_tlsext_host_name(socket_.native_handle(),server.c_str());
        ctx.set_default_verify_paths();
        /*ctx.set_options(boost::asio::ssl::context::default_workarounds
                        | boost::asio::ssl::context::no_sslv2
                        | boost::asio::ssl::context::no_sslv3
                        | boost::asio::ssl::context::no_tlsv1
                        | boost::asio::ssl::context::no_tlsv1_1);*/
        //socket_.set_verify_mode(boost::asio::ssl::verify_none);
        //ctx.load_verify_file("ca.pem");
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
        request_stream << "content-length: " << std::to_string(bodyStartStr.length() + b64ImgStr.length() + bodyEndStr.length()) << "\r\n\r\n";

        request_stream << bodyStartStr << b64ImgStr << bodyEndStr;
        //request_stream << "Accept: */*\r\n";

        // Start an asynchronous resolve to translate the server and service names
        // into a list of endpoints.
        tcp::resolver::query query(server, "https");
        resolver_.async_resolve(query,
                                boost::bind(&client::handle_resolve, this,
                                            boost::asio::placeholders::error,
                                            boost::asio::placeholders::iterator));
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

    //typedef transform_width< binary_from_base64<remove_whitespace<std::string::const_iterator> >, 8, 6 > it_binary_t;
/*
    std::string b64Encode(const std::string& binaryStr)
    {
        typedef insert_linebreaks<base64_from_binary<transform_width<std::string::const_iterator,6,8> >, 72 > it_base64_t;
        unsigned int writePaddChars = (3-binaryStr.length()%3)%3;
        std::string base64(it_base64_t(binaryStr.begin()),it_base64_t(binaryStr.end()));
        base64.append(writePaddChars,'=');
        return base64;
    }
*/
    void handle_resolve(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator)
    {
        if (!err)
        {
            std::cout << "Resolve OK" << "\n";
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
        std::cout << "Verifying " << subject_name << "\n";

        return preverified;
    }

    void handle_connect(const boost::system::error_code& err)
    {
        if (!err)
        {
            std::cout << "Connect OK " << "\n";
            socket_.async_handshake(boost::asio::ssl::stream_base::client,
                                    boost::bind(&client::handle_handshake, this,
                                                boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Connect failed: " << err.message() << "\n";
        }
    }
// a2fqn2y3y2.execute-api.us-east-1.amazonaws.com /prod/mxnet-test-dev-hello /Users/durst/dev/F16-17/vass/mxnet-serverless/testimg.jpg
    void handle_handshake(const boost::system::error_code& error)
    {
        if (!error)
        {
            std::cout << "Handshake OK " << "\n";
            std::cout << "Request: " << "\n";
            const char* header=boost::asio::buffer_cast<const char*>(request_.data());
            std::cout << header << "\n";

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
            std::cout << "Status code: " << status_code << "\n";

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
            while (std::getline(response_stream, header) && header != "\r")
                std::cout << header << "\n";
            std::cout << "\n";

            // Write whatever content we already have to output.
            if (response_.size() > 0)
                std::cout << &response_;

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
            // Write all of the data that has been read so far.
            std::cout << &response_;

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

int main(int argc, char* argv[])
{
    try
    {
        if (argc != 4)
        {
            std::cout << "Usage: async_client <server> <path> <fileContainingPostBody>\n";
            std::cout << "Example:\n";
            std::cout << "  async_client www.boost.org /LICENSE_1_0.txt\n";
            return 1;
        }

        boost::asio::io_service io_service;
        std::ifstream b64File(argv[3]);
        std::stringstream b64ImgStream;
        b64ImgStream << b64File.rdbuf();
        std::string binaryImgStr = b64ImgStream.str();
        //client c(io_service, "www.deelay.me", "/1000/hmpg.net");
        client c(io_service, argv[1], argv[2], binaryImgStr);
        io_service.run();
        std::cout << "should print first\n";
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << "\n";
    }

    return 0;
}
