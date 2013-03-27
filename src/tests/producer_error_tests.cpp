/*
 * producer_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafkaconnect
#include <boost/test/unit_test.hpp>

#include <boost/thread.hpp>

#include "../producer.hpp"

void handle_error(boost::system::error_code const& error, int expected_error, std::string const& expected_message, bool& called)
{
	BOOST_CHECK_EQUAL(expected_error, error.value());
	BOOST_CHECK_EQUAL(expected_message, error.message());
	called = true;
}

BOOST_AUTO_TEST_CASE( invalid_target )
{
	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	bool called = false;
	kafkaconnect::producer producer(io_service, boost::bind(&handle_error, _1,  boost::system::errc::connection_refused, "Connection refused", boost::ref(called)));

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));
	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	BOOST_CHECK(called);

	work.reset();
	io_service.stop();
}

/* TODO: work out why this test doesn't call the exception handler
BOOST_AUTO_TEST_CASE( target_lost )
{
	boost::asio::io_service io_service;
	boost::shared_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
	boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

	boost::asio::ip::tcp::acceptor acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 12345));

	bool called = false;
	kafkaconnect::producer producer(io_service, boost::bind(&handle_error, _1, -1, "", boost::ref(called)));

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	producer.connect("localhost", 12345);

	boost::asio::ip::tcp::socket socket(io_service);
	acceptor.accept(socket);

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));
	BOOST_CHECK_EQUAL(producer.is_connected(), true);

	acceptor.close();
	socket.close();

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));

	producer.send("message", "topic");

	boost::this_thread::sleep(boost::posix_time::milliseconds(100));

	BOOST_CHECK_EQUAL(producer.is_connected(), false);
	BOOST_CHECK(called);

	work.reset();
	io_service.stop();
}
*/
