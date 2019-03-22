package com.streamr.broker.kafka

import com.google.gson.GsonBuilder
import com.streamr.broker.*
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV30
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class KafkaListenerSpec extends Specification {
	String testId
	String groupId

	ExecutorService executor
	List<StreamMessage> receivedMessages
	KafkaListener kafkaListener

	void setup() {
		testId = "KafkaListenerSpec-" + System.currentTimeMillis()
		groupId = "cloud-broker-integration-test-group" + System.currentTimeMillis()

		executor = Executors.newSingleThreadExecutor()
		receivedMessages = []
		kafkaListener = new KafkaListener(Config.KAFKA_HOST, groupId, Config.KAFKA_TOPIC, { msg ->
			// Filter messages belonging to this test in case other messages are published on the topic too
			if (msg.getContent().testId == testId) {
				receivedMessages.add(msg)
			}
		})
	}

	void cleanup() {
		executor.shutdownNow()
	}

	byte[] messageToBytes(Map msg) {
		return new GsonBuilder().create().toJson(msg).bytes
	}

	void "async callback invoked for received records"() {
		when:
		executor.execute(kafkaListener)

		and:
		KafkaDataProducer producer = new KafkaDataProducer(Config.KAFKA_HOST, Config.KAFKA_TOPIC)
		producer.produceToKafka(new StreamrBinaryMessageV28(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				messageToBytes(["message no.": "1", testId: testId])
		))
		producer.produceToKafka(new StreamrBinaryMessageV29(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				messageToBytes(["message no.": "2", testId: testId]),
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
				"msgChainId",
				System.currentTimeMillis() - 1000,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				["message no.": it.toString(), testId: testId],
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"signature"
			)
			producer.produceToKafka(msg)
		}

		then:
		new PollingConditions(timeout: 10, initialDelay: 1.5).eventually {
			ArrayList<String> messageNumbers = new ArrayList<String>()
			for (StreamMessage msg: receivedMessages) {
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

	void "keeps going even when receiving invalid records"() {
		List<StreamMessage> receivedMessages = []
		KafkaListener kafkaListener = new KafkaListener(Config.KAFKA_HOST, groupId, Config.KAFKA_TOPIC, { msg ->
			if (msg.getContent().testId == testId) {
				receivedMessages.add(msg)
			}
		})

		when:
		executor.execute(kafkaListener)

		and:
		KafkaDataProducer producer = new KafkaDataProducer(Config.KAFKA_HOST, Config.KAFKA_TOPIC)
		String content = '{"message no.": "1"}'
		String invalidMsg = "[30,[null,0,1528228173462,0,\"publisherId\",\"1\"],null,27,\\\"$content\\\",0,null]"
		producer.produceToKafka("-0", invalidMsg.getBytes(StandardCharsets.UTF_8))
		producer.produceToKafka(new StreamMessageV30(
				"streamId",
				0,
				System.currentTimeMillis(),
				0,
				"publisherId",
				"msgChainId",
				System.currentTimeMillis() - 1000,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				["message no.": "2", testId: testId],
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"signature"
		))
		then:
		new PollingConditions(timeout: 30, initialDelay: 1.5).eventually {
			ArrayList<String> messageNumbers = new ArrayList<String>()
			for(StreamMessage msg: receivedMessages) {
				messageNumbers.add((String) msg.getContent().get("message no."))
			}
			assert messageNumbers == ["2"]
		}

		cleanup:
		producer?.close()
	}
}
