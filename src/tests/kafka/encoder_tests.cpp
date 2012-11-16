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

#include <string>
#include <vector>
#include <sstream>
#include <iostream>

#include "../../lib/kafka/encoder.hpp"

// test wrapper
namespace kafka { namespace test {
class encoder {
public:
	static std::ostream& message(std::ostream& stream, const std::string message, const uint8_t compression) { return kafka::encoder::message(stream, message, compression); }
	template <typename T> static std::ostream& raw(std::ostream& stream, const T& t) { return kafka::encoder::raw(stream, t); }
};
} }

template <typename IntegerType>
IntegerType bitsToInt(IntegerType& result, const unsigned char* bits, bool little_endian = true)
{
	result = 0;
	if (little_endian) {
		for (int n = sizeof(result); n >= 0; --n) {
			result = (result << 8) + bits[n];
		}
	} else {
		for (unsigned n = 0; n < sizeof(result); ++n) {
			result = (result << 8) + bits[n];
		}
	}
	return result;
}

using namespace kafka::test;

BOOST_AUTO_TEST_SUITE(kafka_encoder)


BOOST_AUTO_TEST_CASE(single_message_test)
{
	std::ostringstream stream;

	std::vector<std::string> messages;
	messages.push_back("test message");

	kafka::encoder::request(stream, "topic", 1, messages, kafka::encoder::COMPRESSION_NONE);

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

	kafka::encoder::request(stream, "topic", 1, messages, kafka::encoder::COMPRESSION_NONE);

	BOOST_CHECK_EQUAL(stream.str().length(), 4 + 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message") + 10 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(3), 2 + 2 + strlen("topic") + 4 + 4 + 10 + strlen("test message") + 10 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(15 + strlen("topic")), 10 + strlen("test message") + 10 + strlen("another message to check"));
}



BOOST_AUTO_TEST_CASE(encode_raw_char)
{
	std::ostringstream stream;
	char value = 0x1;

	encoder::raw(stream, value);

	BOOST_CHECK_EQUAL(stream.str().length(), 1);
	BOOST_CHECK_EQUAL(stream.str().at(0), value);
}

BOOST_AUTO_TEST_CASE(encode_raw_integer)
{
	std::ostringstream stream;
	int value = 0x10203;

	encoder::raw(stream, htonl(value));

	BOOST_CHECK_EQUAL(stream.str().length(), 4);
	BOOST_CHECK_EQUAL(stream.str().at(0), 0);
	BOOST_CHECK_EQUAL(stream.str().at(1), 0x1);
	BOOST_CHECK_EQUAL(stream.str().at(2), 0x2);
	BOOST_CHECK_EQUAL(stream.str().at(3), 0x3);
}

BOOST_AUTO_TEST_CASE(encode_message)
{
	std::string message = "a simple test";
	std::stringstream stream;
	//uint32_t tmp4bytes;
	//uint8_t tmp1byte;

	encoder::message(stream, message, kafka::encoder::COMPRESSION_NONE);

	BOOST_CHECK_EQUAL(stream.str().length(), kafka::encoder::message_format_header_size + message.length());
	//stream >> tmp4bytes; // packet size
	//std::cerr << tmp4bytes << " (package len raw) should be == " << (6 + message.length()) << "\n";
	BOOST_CHECK_EQUAL(stream.str().at(3), 6 + message.length());
	//stream >> tmp1byte;	 // magic number
	BOOST_CHECK_EQUAL(stream.str().at(4), kafka::encoder::message_format_magic_number);
	//stream >> tmp1byte;	 // compression flag
	BOOST_CHECK_EQUAL(stream.str().at(5), kafka::encoder::COMPRESSION_NONE);
	//stream >> tmp4bytes; // CRC32

	for(size_t i = 0; i < message.length(); ++i)
	{
		BOOST_CHECK_EQUAL(stream.str().at(10 + i), message.at(i));
	}
}

BOOST_AUTO_TEST_CASE(encode_gzip_message)
{
	std::string message = "a simple test. The gzipped string should be shorter than this, hopefully. AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
	std::stringstream stream;
	//uint32_t tmp4bytes;
	//uint8_t tmp1byte;

	encoder::message(stream, message, kafka::encoder::COMPRESSION_GZIP);

	//stream >> tmp4bytes;
	//std::cerr << tmp4bytes <<  "\n";
	//std::stringstream(encoded.substr(0,4)) >> len;
	//std::cerr << stream.str().length() << " (package len gzip) should be < " << (kafka::encoder::message_format_header_size + message.length()) << "\n";
	BOOST_CHECK(stream.str().length() < kafka::encoder::message_format_header_size + message.length());
	//std::cerr << (stream.str().at(3) -'A') << " (msg len gzip) should be < " << (6 + message.length()) << "\n";
	BOOST_CHECK(stream.str().at(3) > 7);
	BOOST_CHECK(stream.str().at(3) < 6 + message.length());
	BOOST_CHECK_EQUAL(stream.str().at(4), kafka::encoder::message_format_magic_number);
	BOOST_CHECK_EQUAL(stream.str().at(5), kafka::encoder::COMPRESSION_GZIP);
}

BOOST_AUTO_TEST_SUITE_END()
