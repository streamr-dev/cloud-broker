package com.streamr.broker.kafka;

import com.streamr.broker.StreamrBinaryMessage;
import com.streamr.broker.StreamrBinaryMessageFactory;
import com.streamr.client.protocol.message_layer.*;
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
		StreamMessage oldVersion = streamrBinaryMessage.toStreamrMessage(record.offset(), previousOffset);
		MessageID id = new MessageID(oldVersion.getStreamId(), oldVersion.getStreamPartition(), oldVersion.getTimestamp(),
				record.offset(), oldVersion.getPublisherId(), oldVersion.getMsgChainId());
		StreamMessage.SignatureType signatureType = StreamMessage.SignatureType.SIGNATURE_TYPE_NONE;
		String signature = null;
		if (oldVersion.getVersion() == 29) {
			StreamMessageV29 v29 = (StreamMessageV29) oldVersion;
			signatureType = v29.getSignatureType();
			signature = v29.getSignature();

		}
		return new StreamMessageV30(id, null, oldVersion.getContentType(),
				oldVersion.getSerializedContent(), signatureType, signature);
	}
}
