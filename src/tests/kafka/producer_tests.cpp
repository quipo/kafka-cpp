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
 * producer_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafka
#include <boost/test/unit_test.hpp>

#include <boost/thread.hpp>

#include "../../lib/kafka/producer.hpp"

BOOST_AUTO_TEST_CASE( basic_message_test )
{
	std::string const message   = "so long and thanks for all the fish";
	std::string const topic     = "mice";
	uint32_t const    partition = 42;

	// convenience
	uint32_t const    t_len     = topic.size();
	uint32_t const    m_len     = message.size();

	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::asio::ip::tcp::acceptor acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	kafka::producer producer(kafka::compression_type::none, io_service);
	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	BOOST_CHECK(producer.is_connecting());

	boost::asio::ip::tcp::socket socket(io_service);
	acceptor.accept(socket);

	while(!producer.is_connected())
	{
		boost::this_thread::sleep(boost::posix_time::seconds(1));
	}

	BOOST_CHECK(!producer.is_connecting());

	std::vector<std::string> messages;
	messages.push_back(message);
	kafka::message_ptr_t encoded_msg = producer.encode(messages, topic, partition);
	producer.send(encoded_msg);

	boost::array<char, 1024> buffer;
	boost::system::error_code error;
	size_t len = socket.read_some(boost::asio::buffer(buffer), error);

	BOOST_CHECK_EQUAL(buffer[3], len - 4);                                                // request size is 4 less than total size
	BOOST_CHECK_EQUAL(buffer[5], 0);                                                      // type is produce
	BOOST_CHECK_EQUAL(buffer[7], t_len);                                                  // topic length
	BOOST_CHECK_EQUAL(std::string(&buffer[8], t_len), topic);                             // topic
	BOOST_CHECK_EQUAL(buffer[11 + t_len], partition);                                     // partition
	BOOST_CHECK_EQUAL(buffer[15 + t_len], kafka::message_format_header_size + m_len);     // message set size for produce total of payloads & headers
	BOOST_CHECK_EQUAL(buffer[19 + t_len], kafka::message_format_extra_data_size + m_len); // message length is payload length + magic number + checksum
	BOOST_CHECK_EQUAL(buffer[20 + t_len], kafka::message_format_magic_number);            // magic number
	BOOST_CHECK_EQUAL(buffer[21 + t_len], (uint8_t)kafka::compression_type::none);        // compression value
	BOOST_CHECK_EQUAL(std::string(&buffer[26 + t_len], m_len), message);                  // payload

	work.reset();
	io_service.stop();
}

