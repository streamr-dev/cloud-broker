package com.streamr.broker.kafka

import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageV28
import com.streamr.broker.StreamrBinaryMessageV29
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV28
import com.streamr.client.protocol.message_layer.StreamMessageV29
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

	void "transform() with version 29 returns StreamMessageV29"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessageV29(
				"streamId",
				5,
				666666666,
				10,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				'{"key": "value"}'.bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
				'0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4',
				'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
		)
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamMessageV29 streamMessage = (StreamMessageV29) transformer.transform(record)

		then: "message unaffected"
		streamMessage.getStreamId() == msg.getStreamId()
		streamMessage.getStreamPartition() == msg.getPartition()
		streamMessage.getTimestamp() == msg.getTimestamp()
		streamMessage.getTtl() == msg.getTTL()
		streamMessage.getOffset() == 15
		streamMessage.getContentType().id == msg.getContentType()
		streamMessage.getContent().get("key") == "value"
		streamMessage.getSignatureType().id == msg.getSignatureType().id
		streamMessage.getPublisherId() == msg.getAddress()
		streamMessage.getSignature() == msg.getSignature()
	}

	void "transform() with version 28 returns StreamMessageV28"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessageV28(
				"streamId",
				5,
				666666666,
				10,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				'{"key": "value"}'.bytes,
		)
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamMessageV28 streamMessage = (StreamMessageV28) transformer.transform(record)

		then: "message unaffected"
		streamMessage.getStreamId() == msg.getStreamId()
		streamMessage.getStreamPartition() == msg.getPartition()
		streamMessage.getTimestamp() == msg.getTimestamp()
		streamMessage.getTtl() == msg.getTTL()
		streamMessage.getOffset() == 15
		streamMessage.getContentType().id == msg.getContentType()
		streamMessage.getContent().get("key") == "value"
	}
}
