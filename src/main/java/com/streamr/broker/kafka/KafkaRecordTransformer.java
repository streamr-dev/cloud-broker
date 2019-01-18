package com.streamr.broker.kafka;

import com.streamr.broker.StreamrBinaryMessage;
import com.streamr.broker.StreamrBinaryMessageFactory;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.protocol.message_layer.StreamMessageFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class KafkaRecordTransformer {
	private final Map<String, Long> offsetByStream = new HashMap<>();

	StreamMessage transform(ConsumerRecord<String, byte[]> record) throws IOException {
		String json = new String(record.value(), StandardCharsets.UTF_8);
		if(json.startsWith("[")) { //handle version 30
			return StreamMessageFactory.fromJson(json);
		}
		// handle versions 28 and 29
		StreamrBinaryMessage streamrBinaryMessage = StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(record.value()));
		Long previousOffset = offsetByStream.put(streamrBinaryMessage.getStreamId(), record.offset());
		return streamrBinaryMessage.toStreamrMessage(record.offset(), previousOffset);
	}
}
