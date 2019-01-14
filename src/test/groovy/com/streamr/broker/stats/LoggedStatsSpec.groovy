package com.streamr.broker.stats

import com.streamr.broker.ExampleData
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.*
import org.apache.logging.log4j.core.appender.DefaultErrorHandler
import spock.lang.Specification

import java.text.SimpleDateFormat

class LoggedStatsSpec extends Specification {
	List<String> recordedLogs = []
	Appender testHandler = new TestAppender()
	LoggedStats loggedStats = new LoggedStats()

	void setup() {
		LogManager.getLogger(LoggedStats).addAppender(testHandler)
	}

	void cleanup() {
		LogManager.getLogger(LoggedStats).removeAppender(testHandler)
	}

	void "report() logs 'No new data.' when no events yet occurred"() {
		when:
		loggedStats.report()
		then:
		recordedLogs == ["No new data."]
	}

	void "start() logs info"() {
		when:
		loggedStats.start(120)
		then:
		recordedLogs == ["Statistics logger started. Logging interval is 120 sec(s)."]
	}

	void "report() after some events()"() {
		loggedStats.start(1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_1)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)
		loggedStats.onReadFromKafka(ExampleData.MESSAGE_2)

		loggedStats.onWrittenToCassandra(ExampleData.MESSAGE_1)
		loggedStats.onWrittenToCassandra(ExampleData.MESSAGE_2)

		loggedStats.onCassandraWriteError()

		when:
		loggedStats.report()
		loggedStats.report()

		then:
		// These numbers depend on the size of the version of StreamrBinaryMessage currently used. If a new version with
		// a new size is created, the following will fail. These numbers should be updated.
		recordedLogs == [
			"Statistics logger started. Logging interval is 1 sec(s).",
			"\n\tLast timestamp ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(0))}\n" +
				"\tBackpressure 0.578 kB / 4 events\n" +
				"\tRead throughput 0.867 kB/s or 6 event/s\n" +
				"\tWrite throughput 0.289 kB/s or 2 event/s\n" +
				"\tWrite errors 1",
			"No new data."
		]
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
