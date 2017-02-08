package com.streamr.broker;

import com.streamr.broker.reporter.Reporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

class KafkaRecordHandler implements Closeable {
	private final Map<String, Long> offsetByStream = new HashMap<>();
	private final List<Reporter> reporters;

	KafkaRecordHandler(Reporter... reporters) {
		this.reporters = Arrays.asList(reporters);
	}

	void handle(ConsumerRecord<String, byte[]> consumerRecord) {
		StreamrBinaryMessage streamrBinaryMessage = new StreamrBinaryMessage(ByteBuffer.wrap(consumerRecord.value()));
		Long previousOffset = offsetByStream.put(streamrBinaryMessage.getStreamId(), consumerRecord.offset());

		StreamrBinaryMessageWithKafkaMetadata streamrBinaryMessageWithKafkaMetadata = new StreamrBinaryMessageWithKafkaMetadata(
				streamrBinaryMessage,
				consumerRecord.partition(),
				consumerRecord.offset(),
				previousOffset);

		for (Reporter reporter : reporters) {
			reporter.report(streamrBinaryMessageWithKafkaMetadata);
		}
	}

	@Override
	public void close() {
		for (Reporter reporter : reporters) {
			try {
				reporter.close();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}
}
