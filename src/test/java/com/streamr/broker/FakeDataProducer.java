package com.streamr.broker;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

import static com.streamr.broker.PerformanceConfig.PAYLOAD_SIZE_IN_BYTES;

class FakeDataProducer {
	private final Random r = new Random();
	private final byte[] bytes;

	FakeDataProducer() {
		bytes = new byte[PAYLOAD_SIZE_IN_BYTES];
		r.nextBytes(bytes);
	}

	StreamrBinaryMessageWithKafkaMetadata provideMessage(long offset) {
		return new StreamrBinaryMessageWithKafkaMetadata("streamId",
			1,
			System.currentTimeMillis(),
			1000,
			StreamrBinaryMessageWithKafkaMetadata.CONTENT_TYPE_STRING,
			copyAndCorruptBytes(),
			0,
			offset,
			offset - 1);
	}

	private byte[] copyAndCorruptBytes() {
		byte[] copy = Arrays.copyOf(bytes, bytes.length);
		copy[r.nextInt(copy.length)] = (byte) r.nextInt();
		return copy;
	}

	Function<QueueProducer, Runnable> producer(BrokerProcess brokerProcess) {
		return (QueueProducer queueProducer) -> () -> {
			for (int i = 0; i < PerformanceConfig.NUM_OF_MESSAGES; ++i) {
				queueProducer.accept(provideMessage(i));
			}
			brokerProcess.shutdown();
		};
	}
}
