package com.streamr.broker.kafka;

import com.streamr.client.protocol.message_layer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class KafkaRecordTransformer {

	StreamMessage transform(ConsumerRecord<String, byte[]> record) throws IOException {
		String json = new String(record.value(), StandardCharsets.UTF_8);
		return StreamMessage.fromJson(json);
	}
}
