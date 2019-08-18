package com.streamr.broker.stats

import com.streamr.broker.ExampleData
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.*
import org.apache.logging.log4j.core.appender.DefaultErrorHandler
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.text.SimpleDateFormat

class LoggedStatsSpec extends Specification {
	List<String> recordedLogs = []
	Appender testHandler = new TestAppender()

	void setup() {
		LogManager.getLogger(EventsStats).addAppender(testHandler)
	}

	void cleanup() {
		LogManager.getLogger(EventsStats).removeAppender(testHandler)
	}

	void "report() logs 'No new data.' when no events yet occurred"() {
		LoggedStats loggedStats = new LoggedStats(10)
		when:
		loggedStats.report()
		then:
		recordedLogs == ["No new data."]
	}

	void "start() logs info"() {
		LoggedStats loggedStats = new LoggedStats(120)
		when:
		loggedStats.start()
		then:
		recordedLogs == ["Statistics logger started. Interval is 120 sec(s)."]
	}

	void "report() after some events()"() {
		LoggedStats loggedStats = new LoggedStats(1)
		loggedStats.start()
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)

		loggedStats.onWrittenToCassandra(ExampleData.MESSAGE_1)
		loggedStats.onWrittenToCassandra(ExampleData.MESSAGE_2)

		loggedStats.onCassandraWriteError()

		loggedStats.setReservedCassandraSemaphores(1)
		loggedStats.setReservedMessageSemaphores(100)

		when:
		loggedStats.report()
		loggedStats.report()

		then:
		// These numbers depend on the size of the version of StreamrBinaryMessage currently used. If a new version with
		// a new size is created, the following will fail. These numbers should be updated.
		recordedLogs == [
			"Statistics logger started. Interval is 1 sec(s).",
			"\n\tLast timestamp ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(0))}\n" +
				"\tBackpressure 1.018 kB / 4 events\n" +
				"\tRead throughput 1.527 kB/s or 6 event/s\n" +
				"\tWrite throughput 0.509 kB/s or 2 event/s\n" +
				"\tWrite errors 1\n" +
					"\tReserved message semaphores: 100\n" +
					"\tReserved cassandra semaphores: 1",
			"No new data."
		]
	}

	void "logger is thread-safe"() {
		LoggedStats loggedStats = new LoggedStats(120)
		final int sizeInBytes = ExampleData.MESSAGE_1.sizeInBytes()
		final int THREAD_COUNT = 100
		final int MESSAGE_COUNT = 1000

		// Create 100 threads to concurrently modify counters
		List<Thread> threads = (1..THREAD_COUNT).collect {
			return Thread.start {
				(1..MESSAGE_COUNT).each {
					loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
					loggedStats.onWrittenToCassandra(ExampleData.MESSAGE_1)
					loggedStats.onWrittenToRedis(ExampleData.MESSAGE_1)
				}
			}
		}

		// Wait for all threads to finish
		new PollingConditions(timeout: 5).eventually {
			threads.find { it.isAlive() } == null
		}

		expect:
		loggedStats.getTotalEventsRead() == THREAD_COUNT * MESSAGE_COUNT
		loggedStats.getTotalBytesRead() == THREAD_COUNT * MESSAGE_COUNT * sizeInBytes
		loggedStats.getTotalEventsWritten() == THREAD_COUNT * MESSAGE_COUNT
		loggedStats.getTotalBytesWritten() == THREAD_COUNT * MESSAGE_COUNT * sizeInBytes
	}

	@CompileStatic
	class TestAppender implements Appender {

		@Override
		void append(LogEvent event) {
			recordedLogs.add(event.message.formattedMessage)
		}

		@Override
		String getName() {
			return "TestAppender"
		}

		@Override
		Layout<? extends Serializable> getLayout() {
			return null
		}

		@Override
		boolean ignoreExceptions() {
			return false
		}

		@Override
		ErrorHandler getHandler() {
			return new DefaultErrorHandler(this)
		}

		@Override
		void setHandler(ErrorHandler handler) {}

		@Override
		LifeCycle.State getState() {
			return null
		}

		@Override
		void initialize() {}

		@Override
		void start() {}

		@Override
		void stop() {}

		@Override
		boolean isStarted() {
			return true
		}

		@Override
		boolean isStopped() {
			return false
		}
	}
}
