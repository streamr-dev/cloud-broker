package com.streamr.broker

import com.streamr.broker.stats.Stats
import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue

class QueueProducerSpec extends Specification {
	void "accept(message) puts message to queue"() {
		def queue = new ArrayBlockingQueue(4)
		QueueProducer queueProducer = new QueueProducer(queue, new Stats[0])

		when:
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_1)
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_3)

		then:
		queue.toList() == [ExampleData.MESSAGE_2, ExampleData.MESSAGE_1, ExampleData.MESSAGE_2, ExampleData.MESSAGE_3]
	}

	void "accept(message) invokes Stasts#onReadFromKafka"() {
		def queue = new ArrayBlockingQueue(4)
		def stats = Mock(Stats)
		Stats[] statsArray = [stats]
		QueueProducer queueProducer = new QueueProducer(queue, statsArray)

		when:
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_1)
		queueProducer.accept(ExampleData.MESSAGE_2)
		queueProducer.accept(ExampleData.MESSAGE_3)

		then:
		1 * stats.onReadFromKafka(ExampleData.MESSAGE_1)
		2 * stats.onReadFromKafka(ExampleData.MESSAGE_2)
		1 * stats.onReadFromKafka(ExampleData.MESSAGE_3)
	}
}
