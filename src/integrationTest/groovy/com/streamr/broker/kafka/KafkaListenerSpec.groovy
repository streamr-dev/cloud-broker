package com.streamr.broker.kafka

import com.streamr.broker.Config
import com.streamr.broker.KafkaDataProducer
import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageV28
import com.streamr.broker.StreamrBinaryMessageV29
import com.streamr.broker.StreamrBinaryMessageV30
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class KafkaListenerSpec extends Specification {
	final static String GROUP_ID = "cloud-broker-integration-test-group" + System.currentTimeMillis()
	final static String DATA_TOPIC = "temporary-broker-topic-" + System.currentTimeMillis()
	ExecutorService executor = Executors.newSingleThreadExecutor()

	void setupSpec() {
		// Intentional: Auto-create topic by connecting and producing
		def p = new KafkaDataProducer(Config.KAFKA_HOST, DATA_TOPIC)
		p.produceToKafka(new StreamrBinaryMessageV30(
				"sss",
				0,
				0L,
				0,
				"publisherId",
				0L,
				0,
				0,
				(byte)0,
				"".bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_NONE,
				""))
		p.close()
	}

	void cleanup() {
		executor.shutdownNow()
	}

	void "async callback invoked for received records"() {
		List<StreamrBinaryMessageWithKafkaMetadata> receivedMessages = []
		KafkaListener kafkaListener = new KafkaListener(Config.KAFKA_HOST, GROUP_ID, DATA_TOPIC, { msg ->
			receivedMessages.add(msg)
		})
		def conditions = new PollingConditions(timeout: 10, initialDelay: 1.5)

		when:
		executor.execute(kafkaListener)

		and:
		KafkaDataProducer producer = new KafkaDataProducer(Config.KAFKA_HOST, DATA_TOPIC)
		(1..5).each {
			def msg = new StreamrBinaryMessageV30(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				"publisherId",
				System.currentTimeMillis(),
				0,
				0,
				StreamrBinaryMessage.CONTENT_TYPE_STRING,
				"message no. ${it}".bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_NONE,
				""
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
