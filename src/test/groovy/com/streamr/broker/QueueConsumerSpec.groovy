package com.streamr.broker

import com.streamr.broker.stats.EventsStats
import com.streamr.client.protocol.message_layer.StreamMessage
import groovy.transform.CompileStatic
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class QueueConsumerSpec extends Specification {

	ExecutorService executor = Executors.newSingleThreadExecutor()
	TestReporter reporter1 = new TestReporter()
	TestReporter reporter2 = new TestReporter()
	QueueConsumer queueConsumer

	void setup() {
		BlockingQueue blockingQueue = new ArrayBlockingQueue<>(5)
		blockingQueue.put(ExampleData.MESSAGE_1)
		blockingQueue.put(ExampleData.MESSAGE_2)
		blockingQueue.put(ExampleData.MESSAGE_3)
		queueConsumer = new QueueConsumer(blockingQueue, reporter1, reporter2)
	}

	void cleanup() {
		executor.shutdownNow()
	}

	void "passes messages from queue to reporters"() {
		PollingConditions conditions = new PollingConditions(timeout: 5)

		when:
		executor.execute(queueConsumer)

		then:
		conditions.eventually {
			assert reporter1.receivedMessages == [ExampleData.MESSAGE_1, ExampleData.MESSAGE_2, ExampleData.MESSAGE_3]
			assert reporter2.receivedMessages == [ExampleData.MESSAGE_1, ExampleData.MESSAGE_2, ExampleData.MESSAGE_3]
		}
	}

	void "invokes close() on reporters when interrupted"() {
		PollingConditions conditions = new PollingConditions(timeout: 5)
		executor.execute(queueConsumer)

		when:
		executor.shutdownNow()

		then:
		conditions.eventually {
			assert reporter1.closeInvocations == 1
			assert reporter2.closeInvocations == 1
		}
	}

	@CompileStatic
	class TestReporter implements Reporter {
		List<StreamMessage> receivedMessages = []
		int closeInvocations = 0

		@Override
		void setStats(EventsStats stats) {}

		@Override
		void report(StreamMessage msg) {
			receivedMessages.add(msg)
		}

		@Override
		void close() {
			closeInvocations += 1
		}
	}
}
