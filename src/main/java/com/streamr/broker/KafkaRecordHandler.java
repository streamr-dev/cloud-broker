package com.streamr.broker;

import com.streamr.broker.reporter.Reporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

class KafkaRecordHandler implements Consumer<ConsumerRecord<String, byte[]>> {
	private final Map<String, Long> offsetByStream = new HashMap<>();
	private final List<Reporter> reporters;
	private long counter = 0; // TODO: what to do with these
	private long lastTimestamp = -1;

	KafkaRecordHandler(Reporter... reporters) {
		this.reporters = Arrays.asList(reporters);
	}

	@Override
	public void accept(ConsumerRecord<String, byte[]> consumerRecord) {
		StreamrBinaryMessage streamrBinaryMessage = new StreamrBinaryMessage(ByteBuffer.wrap(consumerRecord.value()));
		Long previousOffset = offsetByStream.put(streamrBinaryMessage.getStreamId(), consumerRecord.offset());

		StreamrBinaryMessageWithKafkaMetadata streamrBinaryMessageWithKafkaMetadata = new StreamrBinaryMessageWithKafkaMetadata(
				ByteBuffer.wrap(consumerRecord.value()),
				consumerRecord.partition(),
				consumerRecord.offset(),
				previousOffset); // TODO: copy constructor

		for (Reporter reporter : reporters) {
			reporter.report(streamrBinaryMessageWithKafkaMetadata);
		}

		++counter;
		lastTimestamp = streamrBinaryMessage.getTimestamp();
	}
}
