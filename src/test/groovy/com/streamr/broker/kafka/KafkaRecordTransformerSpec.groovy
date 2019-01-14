package com.streamr.broker.kafka

import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageV28
import com.streamr.broker.StreamrBinaryMessageV29
import com.streamr.broker.StreamrBinaryMessageV30
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import spock.lang.Specification

class KafkaRecordTransformerSpec extends Specification {

	KafkaRecordTransformer transformer = new KafkaRecordTransformer()

	void "transform() returns StreamrBinaryMessageWithKafkaMetadata with metadata attached"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessageV28(
			"streamId",
			5,
			666666666,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING
		)
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamrBinaryMessageWithKafkaMetadata msgWithMetaData = transformer.transform(record)

		then: "base message unaffected"
		msgWithMetaData.getStreamrBinaryMessage().toBytes() == msg.toBytes()

		and: "meta data attached"
		msgWithMetaData.offset == 15
		msgWithMetaData.previousOffset == null
		msgWithMetaData.kafkaPartition == 7
	}

	void "transform() with version 30 returns StreamrBinaryMessageWithKafkaMetadata with metadata attached"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessageV30(
				"streamId",
				5,
				666666666,
				0,
				"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
				666666600,
				0,
				10,
				StreamrBinaryMessage.CONTENT_TYPE_STRING,
				"".bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
				'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
		)
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamrBinaryMessageWithKafkaMetadata msgWithMetaData = transformer.transform(record)

		then: "base message unaffected"
		msgWithMetaData.getStreamrBinaryMessage().toBytes() == msg.toBytes()

		and: "meta data attached"
		msgWithMetaData.offset == 15
		msgWithMetaData.previousOffset == null
		msgWithMetaData.kafkaPartition == 7
	}

	void "transform() with version 29 returns StreamrBinaryMessageWithKafkaMetadata with metadata attached"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessageV29(
				"streamId",
				5,
				666666666,
				10,
				StreamrBinaryMessage.CONTENT_TYPE_STRING,
				"".bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
				'0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4',
				'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
		)
		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("streamId", 7, 15, null, msg.toBytes())

		when:
		StreamrBinaryMessageWithKafkaMetadata msgWithMetaData = transformer.transform(record)

		then: "base message unaffected"
		msgWithMetaData.getStreamrBinaryMessage().toBytes() == msg.toBytes()

		and: "meta data attached"
		msgWithMetaData.offset == 15
		msgWithMetaData.previousOffset == null
		msgWithMetaData.kafkaPartition == 7
	}

	void "transform() on 2nd invocation with same streamId returns StreamrBinaryMessageWithKafkaMetadata with previous offset attached"() {
		StreamrBinaryMessage msg1 = new StreamrBinaryMessageV28(
			"streamId",
			5,
			666666666,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"".bytes
		)
		ConsumerRecord<String, byte[]> record1 = new ConsumerRecord<>("streamId", 7, 15, null, msg1.toBytes())

		StreamrBinaryMessage msg2 = new StreamrBinaryMessageV28(
			"streamId",
			3,
			999999999,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"".bytes
		)
		ConsumerRecord<String, byte[]> record2 = new ConsumerRecord<>("streamId", 1, 19, null, msg2.toBytes())

		StreamrBinaryMessage msg3 = new StreamrBinaryMessageV28(
			"differentStreamId",
			0,
			777777777,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"".bytes
		)
		ConsumerRecord<String, byte[]> record3 = new ConsumerRecord<>("differentStreamId", 0, 5, null, msg3.toBytes())


		when:
		def res1 = transformer.transform(record1)
		def res2 = transformer.transform(record2)
		def res3 = transformer.transform(record3)

		then:
		res1.previousOffset == null
		res2.previousOffset == 15

		and: "record with different streamId assigned null previousOffset"
		res3.previousOffset == null
	}
}
