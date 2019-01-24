package com.streamr.broker.kafka;

import com.streamr.broker.StreamrBinaryMessage;
import com.streamr.broker.StreamrBinaryMessageFactory;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class KafkaRecordTransformer {
	private final Map<String, Long> offsetByStream = new HashMap<>();

	StreamMessage transform(ConsumerRecord<String, byte[]> record) throws IOException {
		byte[] bytes = record.value();
 		if (bytes.length > 0 && bytes[0] == 0x5b) { //0x5b is the UTF-8 representation of the '[' char
 			// starts with a '[' --> version 30
			String json = new String(record.value(), StandardCharsets.UTF_8);
			return StreamMessage.fromJson(json);
		}
		// handle versions 28 and 29
		StreamrBinaryMessage streamrBinaryMessage = StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(bytes));
		Long previousOffset = offsetByStream.put(streamrBinaryMessage.getStreamId(), record.offset());
		return streamrBinaryMessage.toStreamrMessage(record.offset(), previousOffset);
	}
}
