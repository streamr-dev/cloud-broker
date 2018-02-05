package com.streamr.broker

import com.streamr.broker.stats.Stats
import groovy.transform.CompileStatic
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class QueueConsumerSpec extends Specification {

	ExecutorService executor = Executors.newSingleThreadExecutor()
	TestRepoter reporter1 = new TestRepoter()
	TestRepoter reporter2 = new TestRepoter()
	QueueConsumer queueConsumer

	void setup() {
		BlockingQueue blockingQueue = new ArrayBlockingQueue<>(5)
		blockingQueue.put(ExampleData.MESSAGE_1)
		blockingQueue.put(ExampleData.MESSAGE_2)
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
			reporter1.receivedMessages == [ExampleData.MESSAGE_1, ExampleData.MESSAGE_2]
			reporter2.receivedMessages == [ExampleData.MESSAGE_1, ExampleData.MESSAGE_2]
		}
	}

	void "invokes close() on reporters when interrupted"() {
		PollingConditions conditions = new PollingConditions(timeout: 5)
		executor.execute(queueConsumer)

		when:
		executor.shutdownNow()

		then:
		conditions.eventually {
			reporter1.closeInvocations == 1
			reporter2.closeInvocations == 1
		}
	}

	@CompileStatic
	class TestRepoter implements Reporter {
		List<StreamrBinaryMessageWithKafkaMetadata> receivedMessages = []
		int closeInvocations = 0

		@Override
		void setStats(Stats stats) {}

		@Override
		void report(StreamrBinaryMessageWithKafkaMetadata msg) {
			receivedMessages.add(msg)
		}

		@Override
		void close() {
			closeInvocations += 1
		}
	}
}
