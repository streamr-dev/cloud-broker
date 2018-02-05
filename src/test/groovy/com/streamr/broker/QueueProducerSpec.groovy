package com.streamr.broker

import com.streamr.broker.stats.Stats
import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue

class QueueProducerSpec extends Specification {
	void "accept(message) puts message to queue"() {
		def queue = new ArrayBlockingQueue(3)
		QueueProducer queueProducer = new QueueProducer(queue, Stub(Stats))

		when:
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_1)
		queueProducer.accept(ExampleData.MESSAGE_2)

		then:
		queue.toList() == [ExampleData.MESSAGE_2, ExampleData.MESSAGE_1, ExampleData.MESSAGE_2]
	}

	void "accept(message) invokes Stasts#onReadFromKafka"() {
		def queue = new ArrayBlockingQueue(3)
		def stats = Mock(Stats)
		QueueProducer queueProducer = new QueueProducer(queue, stats)

		when:
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_1)
		queueProducer.accept(ExampleData.MESSAGE_2)

		then:
		1 * stats.onReadFromKafka(ExampleData.MESSAGE_1)
		2 * stats.onReadFromKafka(ExampleData.MESSAGE_2)
	}
}
