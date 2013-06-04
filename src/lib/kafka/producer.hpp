/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
/*
 * producer.hpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_PRODUCER_HPP_
#define KAFKA_PRODUCER_HPP_

#include <string>
#include <vector>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <stdint.h>

#include "encoder.hpp"

namespace kafka {

const uint32_t use_random_partition = 0xFFFFFFFF;

class producer
{
public:
	typedef boost::function<void(boost::system::error_code const&)> error_handler_function;

	producer(compression_type const compression, boost::asio::io_service& io_service, error_handler_function const& error_handler = error_handler_function());
	~producer();

	bool connect(std::string const& hostname, uint16_t const port);
	bool connect(std::string const& hostname, std::string const& servicename);

	bool close();
	bool is_connected() const;
	bool is_connecting() const;

	bool send(std::string const& message, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return send(messages, topic, partition);
	}

	bool send(char const* message, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return send(messages, topic, partition);
	}

	// TODO: replace this with a sending of the buffered data so encode is called prior to send this will allow for decoupling from the encoder
	template <typename List>
	bool send(List const& messages, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		if (!is_connected())
		{
			return false;
		}

		// TODO: make this more efficient with memory allocations.
		boost::asio::streambuf* buffer = new boost::asio::streambuf();
		std::ostream stream(buffer);

		kafka::request(stream, topic, partition, messages, _compression);

		boost::asio::async_write(
			_socket, *buffer,
			boost::bind(&producer::handle_write_request, this, boost::asio::placeholders::error, buffer)
		);

		return true;
	}


private:
	bool _connected;
	bool _connecting;
	compression_type _compression;
	boost::asio::ip::tcp::resolver _resolver;
	boost::asio::ip::tcp::socket _socket;
	error_handler_function _error_handler;

	void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_write_request(const boost::system::error_code& error_code, boost::asio::streambuf* buffer);

	/* Fail Fast Error Handler Braindump
	 *
	 * If an error handler is not provided in the constructor then the default response is to throw
	 * back the boost error_code from asio as a boost system_error exception.
	 *
	 * Most likely this will cause whatever thread you have processing boost io to terminate unless caught.
	 * This is great on debug systems or anything where you use io polling to process any outstanding io,
	 * however if your io thread is seperate and not monitored it is recommended to pass a handler to
	 * the constructor.
	 */
	inline void fail_fast_error_handler(const boost::system::error_code& error_code)
	{
		if (_error_handler.empty())
		{
			throw boost::system::system_error(error_code);
		} else {
			_error_handler(error_code);
		}
	}
};

}

#endif /* KAFKA_PRODUCER_HPP_ */
