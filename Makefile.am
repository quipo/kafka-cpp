## LibKafka
## A C++ shared libray for connecting to Kafka

#
# Warning this is the first time I've made a configure.ac/Makefile.am thing
# Please improve it as I have no idea what I am doing
# @benjamg
#

# Indicate this is not a GNU project and that
# missing files should be automatically created
AUTOMAKE_OPTIONS=foreign

ACLOCAL_AMFLAGS = -I build-aux/m4 ${ACLOCAL_FLAGS}
AM_CPPFLAGS = $(DEPS_CFLAGS) $(BOOST_CPPFLAGS) -I/opt/local/include
#AM_LDFLAGS = -lboost_system$(BOOST_LIB_SUFFIX) $(BOOST_THREADS_LDFLAGS)
AM_LDFLAGS = $(BOOST_THREADS_LDFLAGS) -L/opt/local/lib
#libkafka_LIBS = -L/opt/local/lib -lboost_filesystem-mt -lboost_system-mt -lboost_thread-mt -lkafka
LIBS = -L/opt/local/lib $(BOOST_THREAD_LIBS) \
	-lboost_filesystem-mt -lboost_system-mt \
	-lboost_thread-mt -lboost_iostreams-mt
EXAMPLE_LIBS = $(LIBS) -lkafka
TEST_LIBS = $(DEPS_LIBS) $(EXAMPLE_LIBS) -lboost_unit_test_framework-mt

#
# Shared Library
#

lib_LTLIBRARIES = libkafka.la

libkafka_la_SOURCES = src/lib/kafka/producer.cpp
libkafka_la_LDFLAGS = -version-info $(KAFKA_VERSION)
#-L/opt/local/lib

kafka_includedir = $(includedir)/kafka
kafka_include_HEADERS = src/lib/kafka/producer.hpp \
	src/lib/kafka/encoder.hpp

#
# Examples
#

noinst_PROGRAMS = producer

producer_SOURCES = src/examples/produce.cpp
producer_LDADD = $(DEPS_LIBS) $(EXAMPLE_LIBS)
producer_includedir = @top_srcdir@


#
# Tests
#

check_PROGRAMS = tests/encoder \
	tests/producer \
	tests/producer_error

TESTS = ${check_PROGRAMS}


tests_encoder_SOURCES = src/tests/kafka/encoder_tests.cpp
tests_encoder_LDADD = $(TEST_LIBS)

tests_producer_SOURCES = src/tests/kafka/producer_tests.cpp
tests_producer_LDADD = $(TEST_LIBS)

tests_producer_error_SOURCES = src/tests/kafka/producer_error_tests.cpp
tests_producer_error_LDADD = $(TEST_LIBS)
