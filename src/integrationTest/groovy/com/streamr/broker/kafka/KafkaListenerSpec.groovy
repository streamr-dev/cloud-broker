package com.streamr.broker.kafka

import com.streamr.broker.Config
import com.streamr.broker.KafkaDataProducer
import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageV28
import com.streamr.broker.StreamrBinaryMessageV29
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV30
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
		p.produceToKafka(new StreamrBinaryMessageV29(
				"sss",
				0,
				0L,
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				"".bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_NONE,
				"",
				""))
		p.close()
	}

	void cleanup() {
		executor.shutdownNow()
	}

	void "async callback invoked for received records"() {
		List<StreamMessage> receivedMessages = []
		KafkaListener kafkaListener = new KafkaListener(Config.KAFKA_HOST, GROUP_ID, DATA_TOPIC, { msg ->
			receivedMessages.add(msg)
		})
		def conditions = new PollingConditions(timeout: 10, initialDelay: 1.5)

		when:
		executor.execute(kafkaListener)

		and:
		KafkaDataProducer producer = new KafkaDataProducer(Config.KAFKA_HOST, DATA_TOPIC)
		producer.produceToKafka(new StreamrBinaryMessageV28(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				('{"message no.": "1"}').bytes,
		))
		producer.produceToKafka(new StreamrBinaryMessageV29(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				('{"message no.": "2"}').bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_NONE,
				"",
				""
		))
		(3..5).each {
			def msg = new StreamMessageV30(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				"publisherId",
				System.currentTimeMillis() - 1000,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"message no.": "'+it+'"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"signature"
			)
			producer.produceToKafka(msg)
		}

		then:
		conditions.eventually {
			ArrayList<String> messageNumbers = new ArrayList<String>()
			for(StreamMessage msg: receivedMessages) {
				messageNumbers.add((String) msg.getContent().get("message no."))
			}
			assert messageNumbers == [
			    "1",
				"2",
				"3",
				"4",
				"5",
			]
		}

		cleanup:
		producer?.close()
	}
}
