package com.streamr.broker.kafka

import com.streamr.client.protocol.message_layer.StreamMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import spock.lang.Specification
import com.streamr.client.protocol.message_layer.StreamMessageV30
import com.streamr.client.protocol.message_layer.MessageID
import com.streamr.client.protocol.message_layer.MessageRef

class KafkaRecordTransformerSpec extends Specification {

	KafkaRecordTransformer transformer = new KafkaRecordTransformer()

	void "transform() with version 30 returns StreamMessageV30"() {
		MessageID messageID = new MessageID("streamId", 5, 666666666, 0, "0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4", "msgChainId")
		MessageRef previousMessageRef = new MessageRef(666666600, 0)
		StreamMessageV30 msg = new StreamMessageV30(
				messageID,
				previousMessageRef,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"key": "value"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b')
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamMessage streamMessage = transformer.transform(record)

		then: "message unaffected"
		streamMessage.toBytes() == msg.toBytes()
	}
}
