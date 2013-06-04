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
 * encoder_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafka
#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>

#include "../../lib/kafka/encoder.hpp"

template<typename Data>
Data decode(std::ostringstream const& stream, size_t offset)
{
	std::string const string = stream.str();
	char const* buffer = string.c_str();
	Data const* raw = reinterpret_cast<Data const*>(buffer + offset);
	return *raw;
}

uint8_t decode_byte(std::ostringstream const& stream, size_t offset) { return decode<uint8_t>(stream, offset); }
uint16_t decode_short(std::ostringstream const& stream, size_t offset) { return ntohs(decode<uint16_t>(stream, offset)); }
uint32_t decode_long(std::ostringstream const& stream, size_t offset) { return ntohl(decode<uint32_t>(stream, offset)); }

BOOST_AUTO_TEST_CASE(encode_raw_char)
{
	std::ostringstream stream;
	char value = 0x1;

	kafka::encoder::raw(stream, value);

	BOOST_CHECK_EQUAL(stream.str().length(), 1);
	BOOST_CHECK_EQUAL(stream.str().at(0), value);
}

BOOST_AUTO_TEST_CASE(encode_raw_integer)
{
	std::ostringstream stream;
	int value = 0x10203;

	kafka::encoder::raw(stream, htonl(value));

	BOOST_CHECK_EQUAL(stream.str().length(), 4);
	BOOST_CHECK_EQUAL(stream.str().at(0), 0);
	BOOST_CHECK_EQUAL(stream.str().at(1), 0x1);
	BOOST_CHECK_EQUAL(stream.str().at(2), 0x2);
	BOOST_CHECK_EQUAL(stream.str().at(3), 0x3);
}

BOOST_AUTO_TEST_CASE(encode_message)
{
	std::string message = "a simple test";
	std::ostringstream stream;

	kafka::encoder::payload(stream, message, kafka::compression_type::none);

	BOOST_CHECK_EQUAL(stream.str().length(), kafka::message_format_header_size + message.length());
	BOOST_CHECK_EQUAL(decode_long(stream, 0), kafka::message_format_extra_data_size + message.length());
	BOOST_CHECK_EQUAL(decode_byte(stream, 4), kafka::message_format_magic_number);
	BOOST_CHECK_EQUAL(decode_byte(stream, 5), (uint8_t)kafka::compression_type::none);

	//BOOST_CHECK_EQUAL(decode_long(stream, 6) crc
	BOOST_CHECK_EQUAL(stream.str().substr(10), message);
}

BOOST_AUTO_TEST_CASE(encode_gzip_message)
{
	std::string message = "slightly longer test that may or may not take useful advantage of the compression system";
	std::ostringstream stream;

	kafka::encoder::payload(stream, message, kafka::compression_type::gzip);

	//BOOST_CHECK_EQUAL(stream.str().length(), kafka::message_format_header_size + message.length());
	//BOOST_CHECK_EQUAL(decode_long(stream, 0), kafka::message_format_extra_data_size + message.length());
	BOOST_CHECK_EQUAL(decode_byte(stream, 4), kafka::message_format_magic_number);
	BOOST_CHECK_EQUAL(decode_byte(stream, 5), (uint8_t)kafka::compression_type::gzip);

	//BOOST_CHECK_EQUAL(decode_long(stream, 6) crc
	//BOOST_CHECK_EQUAL(stream.str().substr(10), message);

	// TODO: decode substr(10) and check against original message
}

BOOST_AUTO_TEST_CASE(single_message_test)
{
	std::ostringstream stream;

	std::vector<std::string> messages;
	messages.push_back("test message");

	kafka::request(stream, "topic", 1, messages, kafka::compression_type::none);

	BOOST_CHECK_EQUAL(stream.str().length(), 4 + 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(3), 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(6), 0);
	BOOST_CHECK_EQUAL(stream.str().at(7), strlen("topic"));
	BOOST_CHECK_EQUAL(stream.str().at(8), 't');
	BOOST_CHECK_EQUAL(stream.str().at(8 + strlen("topic") - 1), 'c');
	BOOST_CHECK_EQUAL(stream.str().at(11 + strlen("topic")), 1);
	BOOST_CHECK_EQUAL(stream.str().at(15 + strlen("topic")), 10 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(16 + strlen("topic")), 0);
	BOOST_CHECK_EQUAL(stream.str().at(26 + strlen("topic")), 't');
}

BOOST_AUTO_TEST_CASE(multiple_message_test)
{
	std::ostringstream stream;

	std::vector<std::string> messages;
	messages.push_back("test message");
	messages.push_back("another message to check");

	kafka::request(stream, "topic", 1, messages, kafka::compression_type::none);

	BOOST_CHECK_EQUAL(stream.str().length(), 4 + 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message") + 10 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(3), 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message") + 10 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(15 + strlen("topic")), 10 + strlen("test message") + 10 + strlen("another message to check"));
}
