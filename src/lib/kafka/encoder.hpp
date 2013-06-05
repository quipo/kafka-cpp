
#ifndef KAFKA_ENCODER_HPP_
#define KAFKA_ENCODER_HPP_

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <boost/crc.hpp>
#include <boost/foreach.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>

namespace kafka {

enum class compression_type : uint8_t {
	none,         // 0
	gzip,         // 1
	snappy        // 2
};

enum class request_type : uint16_t {
	produce,      // 0    Send a group of messages to a topic and partition.
	fetch,        // 1    Fetch a group of messages from a topic and partition.
	multifetch,   // 2    Multiple FETCH requests, chained together
	multiproduce, // 3    Multiple PRODUCE requests, chained together
	offsets       // 4    Find offsets before a certain time (this can be a bit misleading, please read the details of this request
};

const uint8_t message_format_magic_number = 1;
const uint8_t message_format_extra_data_size = 1 + 1 + 4; // magic number, compression flag, crc32
const uint8_t message_format_header_size = message_format_extra_data_size + 4; // len

namespace encoder {

template <typename T>
inline std::string compress_gzip(T const& payload)
{
	std::ostringstream oss;
	{
		boost::iostreams::filtering_stream<boost::iostreams::output> f;
		f.push(boost::iostreams::gzip_compressor());
		f.push(oss);
		f << payload;
		f.flush();
		boost::iostreams::close(f);
	} // gzip_compressor flushes when f goes out of scope
	return oss.str();
}

/*
template <typename T>
inline void decompress_gzip(T& o, std::string const& s)
{
	std::istringstream iss(s);
	boost::iostreams::filtering_stream<boost::iostreams::input> f;
	f.push(boost::iostreams::gzip_decompressor());
	f.push(iss);
	f >> o;
}
*/

inline std::string compress(std::string const& payload, compression_type const& compression)
{
	switch (compression) {
		case compression_type::none:
			return payload;
		case compression_type::gzip:
			return compress_gzip(payload);
		default:
			throw std::invalid_argument("Unsupported compression type used, currently only none and gzip are supported");
	}
}

template <typename Data>
inline std::ostream& raw(std::ostream& stream, const Data& data)
{
	stream.write(reinterpret_cast<const char*>(&data), sizeof(Data));
	return stream;
}

std::ostream& payload(std::ostream& stream, std::string const& payload, compression_type const& compression)
{
	std::string msg = compress(payload, compression);

	// Message format is ... message & data size (4 bytes)
	raw(stream, htonl(message_format_extra_data_size + msg.length()));

	// ... magic number (1 byte)
	encoder::raw(stream, message_format_magic_number);

	// ... compression flag (1 byte)
	encoder::raw(stream, compression);

	// ... string crc32 (4 bytes)
	boost::crc_32_type result;
	result.process_bytes(msg.c_str(), msg.length());
	raw(stream, htonl(result.checksum()));

	// ... message payload string bytes
	stream << msg;

	return stream;
}

} // namespace encoder

template <typename List>
void request(std::ostream& stream, std::string const& topic, uint32_t const partition, const List& messages, compression_type const& compression = compression_type::none)
{
	std::ostringstream message_buffer;

	// Due to the dreadful way compression is handled in 0.7 we bunch the messages before compressing and fake a single message
	BOOST_FOREACH(std::string const& msg, messages)
	{
		encoder::payload(message_buffer, msg, compression_type::none);
	}

	// if compression is on, compress the message_set and wrap it into a new message
	if (compression != compression_type::none) {
		// Convert streambuf to std::string, process it and clear buffer
		std::ostringstream tmp;
		encoder::payload(tmp, message_buffer.str(), compression);

		//std::swap(message_buffer, tmp); -- broken under gcc 4.7
		message_buffer.str(tmp.str());
	}

	std::string message_set = message_buffer.str();
	uint32_t messageset_size = message_set.size();
	uint32_t request_size = 2 + 2 + topic.size() + 4 + 4 + messageset_size;

	// Packet format is ... request size (4 bytes)
	encoder::raw(stream, htonl(request_size));

	// ... request_type (2 bytes)
	uint16_t type = static_cast<uint16_t>(request_type::produce); // some compilers don't like enum class yet
	encoder::raw(stream, htons(type));

	// ... topic string size (2 bytes) & topic string
	encoder::raw(stream, htons(topic.size()));
	stream << topic;

	// ... partition (4 bytes)
	encoder::raw(stream, htonl(partition));

	// ... message set size (4 bytes) and message set
	encoder::raw(stream, htonl(messageset_size));

	// finally, transfer the payload from the temporary stream buffer
	stream << message_set;
}

} // namespace kafka

#endif /* KAFKA_ENCODER_HPP_ */
