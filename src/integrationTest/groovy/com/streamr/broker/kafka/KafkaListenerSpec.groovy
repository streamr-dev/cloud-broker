package com.streamr.broker.kafka

import com.streamr.broker.KafkaDataProducer
import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class KafkaListenerSpec extends Specification {
	final static String ZOOKEEPER_HOST = "127.0.0.1:9092"
	final static String GROUP_ID = "cloud-broker-integration-test-group" + System.currentTimeMillis()
	final static String DATA_TOPIC = "temporary-broker-topic-" + System.currentTimeMillis()

	ExecutorService executor = Executors.newSingleThreadExecutor()

	void setupSpec() {
		// Auto-initialize topic by connecting and producing
		def p = new KafkaDataProducer(ZOOKEEPER_HOST, DATA_TOPIC)
		p.produceToKafka(new StreamrBinaryMessage("sss", 0, 0, 0, (byte)0, "".bytes))
		p.close()
	}

	void cleanup() {
		executor.shutdownNow()
	}

	void "async callback invoked for received records"() {
		List<StreamrBinaryMessageWithKafkaMetadata> receivedMessages = []
		KafkaListener kafkaListener = new KafkaListener(ZOOKEEPER_HOST, GROUP_ID, DATA_TOPIC, { msg ->
			receivedMessages.add(msg)
		})
		def conditions = new PollingConditions(timeout: 10, initialDelay: 1.5)

		when:
		executor.execute(kafkaListener)

		and:
		KafkaDataProducer producer = new KafkaDataProducer(ZOOKEEPER_HOST, DATA_TOPIC)
		(1..5).each {
			def msg = new StreamrBinaryMessage(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_STRING,
				"message no. ${it}".bytes
			)
			producer.produceToKafka(msg)
		}

		then:
		conditions.eventually {
			assert receivedMessages*.toString() == [
			    "message no. 1",
				"message no. 2",
				"message no. 3",
				"message no. 4",
				"message no. 5",
			]
		}

		cleanup:
		producer?.close()
	}
}
