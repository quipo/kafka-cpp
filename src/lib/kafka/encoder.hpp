
#ifndef KAFKA_ENCODER_HPP_
#define KAFKA_ENCODER_HPP_

#include <ostream>
#include <string>
#include <stdint.h>

#include <boost/crc.hpp>
#include <boost/foreach.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>

namespace kafka {
namespace encoder {

const uint8_t message_format_magic_number = 1;
const uint8_t message_format_extra_data_size = 1 + 1 + 4; // magic number, compression flag, crc32
const uint8_t message_format_header_size = message_format_extra_data_size + 4; // len

const uint8_t COMPRESSION_NONE   = 0;
const uint8_t COMPRESSION_GZIP   = 1;
const uint8_t COMPRESSION_SNAPPY = 2;

template <typename T> inline std::string gzipCompress(const T & o) {
        std::ostringstream oss;
        {
                boost::iostreams::filtering_stream<boost::iostreams::output> f;
                f.push(boost::iostreams::gzip_compressor());
                f.push(oss);
                //bar::binary_oarchive oa(f);
                //oa << o;
                f << o;
                f.flush();
                boost::iostreams::close(f);
        } // gzip_compressor flushes when f goes out of scope
        return oss.str();
}

template <typename T> inline void gzipDecompress(T & o, const std::string& s) {
        std::istringstream iss(s);
        boost::iostreams::filtering_stream<boost::iostreams::input> f;
        f.push(boost::iostreams::gzip_decompressor());
        f.push(iss);
        //bar::binary_iarchive ia(f);
        //ia >> o;
        f >> o;
        //boost::iostreams::close(f);
}

template <typename Data>
std::ostream& raw(std::ostream& stream, const Data& data)
{
	stream.write(reinterpret_cast<const char*>(&data), sizeof(Data));
	return stream;
}

std::string compress(const std::string msg_in, const uint8_t compression)
{
	std::string compressed;
	boost::iostreams::filtering_ostream compressingStream;
	switch (compression) {
		case COMPRESSION_NONE:
			return msg_in;
		case COMPRESSION_GZIP:
			return gzipCompress(msg_in);
			/*
			try
			{
				compressingStream.push(boost::iostreams::gzip_compressor());
				compressingStream.push(boost::iostreams::back_inserter(compressed));
				compressingStream << msg_in;
				boost::iostreams::close(compressingStream);
				return compressed;
			}
			catch(boost::iostreams::gzip_error& error)
			{
				std::cerr << "Caught gzip error with code: " << error.error() << "\n";
				std::cerr << "data_error: " << boost::iostreams::gzip::data_error << "\n";
			}
			*/
		default:
			//FIXME
			//throw exception
			return "";
	}
}

std::ostream& message(std::ostream& stream, const std::string msg_in, const uint8_t compression)
{
	std::string msg = compress(msg_in, compression);

	// Message format is ... message & data size (4 bytes)
	raw(stream, htonl(message_format_extra_data_size + msg.length()));

	// ... magic number (1 byte)
	stream << message_format_magic_number;

	// ... compression flag (1 byte)
	stream << compression;

	// ... string crc32 (4 bytes)
	boost::crc_32_type result;
	result.process_bytes(msg.c_str(), msg.length());
	raw(stream, htonl(result.checksum()));

	// ... message payload string bytes
	stream << msg;

	return stream;
}

template <typename List>
void request(std::ostream& stream, const std::string& topic, const uint32_t partition, const List& messages, const uint8_t compression = COMPRESSION_NONE)
{
	std::ostringstream tmpBuf;

	BOOST_FOREACH(const std::string& msg, messages)
	{
		message(tmpBuf, msg, COMPRESSION_NONE);
	}

	// if compression is on, compress the message_set and wrap it into a new message
	if (compression != COMPRESSION_NONE) {
		// Convert streambuf to std::string, process it and clear buffer
		std::string message_set = tmpBuf.str();
		std::cout << "----------------\n" << message_set << "\n";
		tmpBuf.str("");
		std::cout << tmpBuf.str() << "\n --- \n";
		message(tmpBuf, message_set, compression);
	}

	std::string message_set = tmpBuf.str();

	uint32_t messageset_size = message_set.size();
	uint32_t request_size = 2 + 2 + topic.size() + 4 + 4 + messageset_size;

	// Packet format is ... request size (4 bytes)
	raw(stream, htonl(request_size));

	// ... magic number (2 bytes)
	raw(stream, htons(message_format_magic_number));

	// ... topic string size (2 bytes) & topic string
	raw(stream, htons(topic.size()));
	stream << topic;

	// ... partition (4 bytes)
	raw(stream, htonl(partition));

	// ... message set size (4 bytes) and message set
	raw(stream, htonl(messageset_size));

	// finally, transfer the payload from the temporary stream buffer
	stream << message_set;
}


}
}

#endif /* KAFKA_ENCODER_HPP_ */