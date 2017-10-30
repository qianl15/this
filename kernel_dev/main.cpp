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

using boost::asio::ip::tcp;

class client
{
public:
    client(boost::asio::io_service& io_service,
           const std::string& server, const std::string& path, const std::string& b64ImgStr)
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
        // Form the request. We specify the "Connection: close" header so that the
        // server will close the socket after transmitting the response. This will
        // allow us to treat all data up until the EOF as the content.
        std::ostream request_stream(&request_);
        request_stream << "PATH " << path << " HTTP/1.1\r\n";
        request_stream << "Host: " << server << "\r\n";
        request_stream << "content-type: application/json;charset=UTF-8\r\n";
        request_stream << "content-length: " << std::to_string(b64ImgStr.length()) << "\r\n";
        request_stream << "Connection: close\r\n\r\n";


        request_stream << "{\"httpMethod\":\"POST\",\"pathWithQueryString\":\"/mxnet-test-dev-hello\",\"body\":\"{\\\"b64Img\\\": \\\""
                       << b64ImgStr << "\\\"}\",\"headers\":{},\"stageVariables\":{},\"withAuthorization\":false}\r\n";
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

    /*
    void handle_resolve(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator)
    {
        if (!err)
        {
            socket_.set_verify_mode(boost::asio::ssl::verify_peer);
            socket_.set_verify_callback(
                    boost::bind(&client::verify_certificate, this, _1, _2));
            // Attempt a connection to the first endpoint in the list. Each endpoint
            // will be tried until we successfully establish a connection.
            tcp::endpoint endpoint = *endpoint_iterator;
            socket_.lowest_layer().async_connect(endpoint,
                                  boost::bind(&client::handle_connect, this,
                                              boost::asio::placeholders::error, ++endpoint_iterator));
        }
        else
        {
            std::cout << "Error: " << err.message() << "\n";
        }
        //-DOPENSSL_ROOT_DIR=/usr/local/opt/openssl -DOPENSSL_INCLUDE_DIR==/usr/local/opt/openssl/lib
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

    void handle_connect(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator)
    {
        if (!err)
        {
            //socket_.async_handshake(boost::asio::ssl::stream_base::handshake_type::client);
            // The connection was successful. Send the request.
            boost::asio::async_write(socket_, request_,
                                     boost::bind(&client::handle_write_request, this,
                                                 boost::asio::placeholders::error));
        }
        else if (endpoint_iterator != tcp::resolver::iterator())
        {
            // The connection failed. Try the next endpoint in the list.
            socket_.lowest_layer().close();
            tcp::endpoint endpoint = *endpoint_iterator;
            socket_.lowest_layer().async_connect(endpoint,
                                  boost::bind(&client::handle_connect, this,
                                              boost::asio::placeholders::error, ++endpoint_iterator));
        }
        else
        {
            std::cout << "Error: " << err.message() << "\n";
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
            std::cout << "Error: " << err.message() << "\n";
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

            // Read the response headers, which are terminated by a blank line.
            boost::asio::async_read_until(socket_, response_, "\r\n\r\n",
                                          boost::bind(&client::handle_read_headers, this,
                                                      boost::asio::placeholders::error));
        }
        else
        {
            std::cout << "Error: " << err << "\n";
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
     */

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
        std::string b64ImgStr = b64ImgStream.str();
        //client c(io_service, "www.deelay.me", "/1000/hmpg.net");
        client c(io_service, argv[1], argv[2], b64ImgStr);
        io_service.run();
        std::cout << "should print first\n";
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << "\n";
    }

    return 0;
}
