package com.streamr.broker.kafka

import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import spock.lang.Specification

class KafkaRecordTransformerSpec extends Specification {

	KafkaRecordTransformer transformer = new KafkaRecordTransformer()

	void "transform() returns StreamrBinaryMessageWithKafkaMetadata with metadata attached"() {
		StreamrBinaryMessage msg = new StreamrBinaryMessage(
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
		msgWithMetaData.toBytes() == msg.toBytes()

		and: "meta data attached"
		msgWithMetaData.offset == 15
		msgWithMetaData.previousOffset == null
		msgWithMetaData.kafkaPartition == 7
	}

	void "transform() on 2nd invocation with same streamId returns StreamrBinaryMessageWithKafkaMetadata with previous offset attached"() {
		StreamrBinaryMessage msg1 = new StreamrBinaryMessage(
			"streamId",
			5,
			666666666,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"".bytes
		)
		ConsumerRecord<String, byte[]> record1 = new ConsumerRecord<>("streamId", 7, 15, null, msg1.toBytes())

		StreamrBinaryMessage msg2 = new StreamrBinaryMessage(
			"streamId",
			3,
			999999999,
			10,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"".bytes
		)
		ConsumerRecord<String, byte[]> record2 = new ConsumerRecord<>("streamId", 1, 19, null, msg2.toBytes())

		StreamrBinaryMessage msg3 = new StreamrBinaryMessage(
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
