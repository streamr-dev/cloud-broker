package com.streamr.broker.kafka;

import com.streamr.broker.StreamrBinaryMessage;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

class KafkaRecordTransformer {
	private final Map<String, Long> offsetByStream = new HashMap<>();

	StreamrBinaryMessageWithKafkaMetadata transform(ConsumerRecord<String, byte[]> record) {
		StreamrBinaryMessage streamrBinaryMessage = StreamrBinaryMessage.from(ByteBuffer.wrap(record.value()));
		Long previousOffset = offsetByStream.put(streamrBinaryMessage.getStreamId(), record.offset());
		return new StreamrBinaryMessageWithKafkaMetadata(streamrBinaryMessage, record.partition(), record.offset(),
			previousOffset);
	}
}
