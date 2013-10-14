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
#include <memory>
#include <functional>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <stdint.h>

#include "encoder.hpp"

namespace kafka {

const uint32_t use_random_partition = 0xFFFFFFFF;

class producer;
/*
 * Class that encapsulates a kafka encoded message.
 * It can only be created by the producer using the encode functions.
 *
 */
class message
{

	// a message object should always contain an encoded message.
	// We could allow copy construction and assignment, but we can
	// not allow move as it will leave the original message object "empty".
	// Deleting the move constructor will result in deleted implicitly
	// declared copy constructor and assignment operators, so if we really
	// do need those, we will need to explicitly provide them. At the
	// moment there is no need for them.
	message( message && ) = delete;

private:
	friend class producer;

	message(std::string in): content{in}{};
	std::string content;
};
typedef std::shared_ptr<message> message_ptr_t;

class producer
{
public:
	typedef void(*connect_error_handler_function)(boost::system::error_code const&);
	typedef void(*send_error_handler_function)(boost::system::error_code const&, message_ptr_t msg_ptr);

	producer(compression_type const compression, boost::asio::io_service& io_service);
	~producer();

	bool connect(std::string const& hostname
			, uint16_t const port
			, connect_error_handler_function error_handler = nullptr);
	bool connect(std::string const& hostname
			, std::string const& servicename
			, connect_error_handler_function error_handler = nullptr);

	bool close();
	bool is_connected() const;
	bool is_connecting() const;


	/*
	 * Function to encode the required content into a kafka message object.
	 *
	 */
	message_ptr_t encode(std::string const& message, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return encode(messages, topic, partition);
	}

	message_ptr_t encode(char const* message, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return encode(messages, topic, partition);
	}

	template <typename List>
	message_ptr_t encode(List const& messages, std::string const& topic, uint32_t const partition = use_random_partition)
	{
		std::stringstream buffer;
		// TODO: make this more efficient with memory allocations.
		kafka::request(buffer, topic, partition, messages, _compression);

		message_ptr_t msg_ptr{new kafka::message{buffer.str()}};
		return msg_ptr;
	}

	/*
	 * Function that asynchronously sends a kafka message over an existing connection.
	 *
	 * Optional error handler is called if asycnhronous call fails. If there is no
	 * error_handler provided, and async call fails, boost system_error exception will be thrown
	 * containing the boost error code.
	 *
	 * \param 	msg_ptr			a kafka encoded message
	 * \param 	error_handler	function to call if async call fails
	 * \returns	true if async_write is successfully started, false otherwise
	 *
	 */
	bool send(message_ptr_t msg_ptr, send_error_handler_function error_handler = nullptr)
	{
		if (!is_connected() || !msg_ptr.get())
		{
			return false;
		}

		// TODO: make this more efficient with memory allocations.
		boost::asio::streambuf* buffer = new boost::asio::streambuf();
		std::ostream stream(buffer);

		stream << msg_ptr->content;

		// async_write will not detect far end closed connection.
		// If this is a problem, consider using write in a separate thread. This
		// will require changing the class to be thread safe.
		boost::asio::async_write(
				_socket
				, *buffer
				, boost::bind(&producer::handle_write_request
							, this
							, boost::asio::placeholders::error
							, boost::asio::placeholders::bytes_transferred
							, msg_ptr
							, error_handler
				)
		);

		return true;
	}


private:
	bool _connected;
	bool _connecting;
	compression_type _compression;
	boost::asio::ip::tcp::resolver _resolver;
	boost::asio::ip::tcp::socket _socket;


	void handle_resolve(const boost::system::error_code& error_code
			, boost::asio::ip::tcp::resolver::iterator endpoints
			, connect_error_handler_function error_handler);
	void handle_connect(const boost::system::error_code& error_code
			, boost::asio::ip::tcp::resolver::iterator endpoints
			, connect_error_handler_function error_handler);
	void handle_write_request(const boost::system::error_code& error_code
			, std::size_t bytes_transferred
			, message_ptr_t msg_ptr
			, send_error_handler_function error_handler);

	/*
	 * Handler for our dummy read. If the far end closes connection, the dummy
	 * read will fail and we can adjust the connection status.
	 * Any async_writes in progress will still return success (as async_read does not
	 * seem to wait for ACK), so there might be some kafka messages lost
	 *
	 */
	void handle_dummy_read(std::shared_ptr<boost::array<char, 1>>
			, const boost::system::error_code& error_code)
	{
		if (error_code)
		{
				// The connection closed
				_connected = false;
		}
		else
		{
			// strange, we should not have received anything over this connection
			// start the dummy read again
			std::shared_ptr<boost::array<char, 1>> buf(new boost::array<char, 1>);
			boost::asio::async_read(_socket
								, boost::asio::buffer(*buf)
								, boost::bind(&producer::handle_dummy_read
												, this
												, buf
												, boost::asio::placeholders::error
											  )
								);
		}
	}


};

}

#endif /* KAFKA_PRODUCER_HPP_ */
