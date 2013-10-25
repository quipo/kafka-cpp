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
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "../../lib/kafka/encoder.hpp"
#include "../../lib/kafka/producer.hpp"

bool handle_invalid_target_error_called{false};

struct consumer
{
	boost::asio::io_service _io_service;
	boost::asio::io_service::work _work;
	boost::asio::ip::tcp::acceptor _acceptor;
	boost::asio::ip::tcp::socket _socket;

	boost::thread _bt;

	consumer():
		  _work(_io_service)
		, _acceptor(_io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345))
		, _socket(_io_service)
		, _bt(boost::bind(&boost::asio::io_service::run, &_io_service))
	{};

	void accept_handler(const boost::system::error_code& error)
	{
		//do nothing
	}
	void async_accept()
	{
		_acceptor.async_accept(_socket
				, boost::bind(
						&consumer::accept_handler
							, this
							, boost::asio::placeholders::error
							)
				);

	}

	~consumer()
	{
		_acceptor.close();
		_socket.close();
		_io_service.stop();
		_bt.join();
	}

};
void handle_invalid_target_error(boost::system::error_code const& error)
{
	BOOST_CHECK_EQUAL( boost::asio::error::connection_refused, error.value());
	handle_invalid_target_error_called = true;
}
/*
 * sending to invalid target should not work
 * This needs resolution of https://svn.boost.org/trac/boost/ticket/8795 to work
 *
 */
BOOST_AUTO_TEST_CASE( invalid_target )
{
	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	kafka::producer producer(kafka::compression_type::none, io_service );

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345, handle_invalid_target_error);

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));

	bool temp = producer.is_connected();
	BOOST_CHECK_EQUAL(temp, false);
	BOOST_CHECK(handle_invalid_target_error_called);

	work.reset();
	io_service.stop();
}
/*
 * If far end closes connection, we should detect it
 */
BOOST_AUTO_TEST_CASE( target_lost )
{

	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));
	kafka::producer producer(kafka::compression_type::none, io_service);

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	std::shared_ptr<consumer> far_end(new consumer());
	far_end->async_accept();

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));
	BOOST_CHECK_EQUAL(producer.is_connected(), true);

	far_end.reset();

	kafka::message_ptr_t encoded_msg = producer.encode("message", "topic");
	BOOST_CHECK_EQUAL(producer.send(encoded_msg),false);

	// do we detect error
	BOOST_CHECK_EQUAL(producer.is_connected(), false);

	//try and reconnect;
	far_end.reset(new consumer());
	far_end->async_accept();

	producer.connect("localhost", 12345);
	sleep(3);
	BOOST_CHECK_EQUAL(producer.is_connected(), true);

	//send a message
	std::string message{"peanut butter"};
	std::string topic{"perfection"};
	uint32_t const    partition = 42;

	// convenience
	uint32_t const    t_len     = topic.size();
	uint32_t const    m_len     = message.size();

	encoded_msg = producer.encode(message, topic, partition);
	producer.send(encoded_msg);

	boost::array<char, 1024> buffer;
	boost::system::error_code error;

	size_t len = far_end->_socket.read_some(boost::asio::buffer(buffer), error);

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

/*
 *  we should report an error trying to send an empty kafka::message_ptr_t
 */
BOOST_AUTO_TEST_CASE( empty_message )
{
	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	boost::asio::ip::tcp::acceptor acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345));

	kafka::producer producer(kafka::compression_type::none, io_service);

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	boost::asio::ip::tcp::socket socket(io_service);
	acceptor.accept(socket);

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));
	BOOST_CHECK_EQUAL(producer.is_connected(), true);

	kafka::message_ptr_t encoded_msg;
	BOOST_CHECK_EQUAL(producer.send(encoded_msg),false);

	work.reset();
	io_service.stop();
}

/*
 * TODO add tests that detect send failures. Not sure how to reproduce these at the moment
 */
//*/
