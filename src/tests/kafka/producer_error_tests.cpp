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

	boost::asio::ip::tcp::acceptor acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345));

	kafka::producer producer(kafka::compression_type::none, io_service);

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	boost::asio::ip::tcp::socket socket(io_service);
	acceptor.accept(socket);

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));
	BOOST_CHECK_EQUAL(producer.is_connected(), true);

	acceptor.close();
	socket.close();

	kafka::message_ptr_t encoded_msg = producer.encode("message", "topic");
	BOOST_CHECK_EQUAL(producer.send(encoded_msg),false);

	// do we detect error
	BOOST_CHECK_EQUAL(producer.is_connected(), false);


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
