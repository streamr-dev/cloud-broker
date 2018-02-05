package com.streamr.broker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.Function;

import static com.streamr.broker.PerformanceTestConfiguration.STREAM_IDS;

class RandomDataProducer {
	private static final Logger log = LogManager.getLogger();

	private final SplittableRandom random = new SplittableRandom();
	private final Random random2 = new Random();
	private long totalBytes = 0;

	StreamrBinaryMessageWithKafkaMetadata provideMessage(long offset) {
		return new StreamrBinaryMessageWithKafkaMetadata(STREAM_IDS[random2.nextInt(STREAM_IDS.length)],
			0,
			System.currentTimeMillis(),
			1000,
			StreamrBinaryMessageWithKafkaMetadata.CONTENT_TYPE_STRING,
			generatePayload(),
			0,
			offset,
			offset - 1);
	}

	private byte[] generatePayload() {
		byte[] payload;
		if (random.nextDouble() < 0.9) {
			payload = new byte[random.nextInt(PerformanceTestConfiguration.SMALL_PAYLOAD_RANGE[0], PerformanceTestConfiguration.SMALL_PAYLOAD_RANGE[1])];
		} else {
			payload = new byte[random.nextInt(PerformanceTestConfiguration.LARGE_PAYLOAD_RANGE[0], PerformanceTestConfiguration.LARGE_PAYLOAD_RANGE[1])];
		}
		random2.nextBytes(payload);
		totalBytes += payload.length;
		return payload;
	}

	Function<QueueProducer, Runnable> producer(BrokerProcess brokerProcess) {
		return (QueueProducer queueProducer) -> () -> {
			for (int i = 0; i < PerformanceTestConfiguration.NUM_OF_MESSAGES; ++i) {
				queueProducer.accept(provideMessage(i));
			}
			logTotalByesGenerated();
			brokerProcess.shutdown();
		};
	}

	void logTotalByesGenerated() {
		log.info("Generated a total of {} MB of random data.", totalBytes / (1000*1000));
	}
}
