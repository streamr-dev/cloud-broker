package com.streamr.broker;

import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.protocol.message_layer.StreamMessageV30;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.Function;

import static com.streamr.broker.PerformanceTestConfiguration.STREAM_IDS;

class RandomDataProducer {
	private static final Logger log = LogManager.getLogger();

	private final SplittableRandom random = new SplittableRandom();
	private final Random random2 = new Random();
	private long totalBytes = 0;

	StreamMessage provideMessage(long offset) {
		try {
			return new StreamMessageV30(
					STREAM_IDS[random2.nextInt(STREAM_IDS.length)],
					0,
					System.currentTimeMillis(),
					0,
					"publisherId",
					"msgChainId",
					0L,
					0L,
					StreamMessage.ContentType.CONTENT_TYPE_JSON,
					"{\"payload\":\""+generatePayload()+"\"}",
					StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
					"signature"
			);
		} catch (IOException e) {
			log.error(e);
			throw new RuntimeException(e);
		}
	}

	private String generatePayload() {
		byte[] payload;
		if (random.nextDouble() < 0.9) {
			payload = new byte[random.nextInt(PerformanceTestConfiguration.SMALL_PAYLOAD_RANGE[0], PerformanceTestConfiguration.SMALL_PAYLOAD_RANGE[1])];
		} else {
			payload = new byte[random.nextInt(PerformanceTestConfiguration.LARGE_PAYLOAD_RANGE[0], PerformanceTestConfiguration.LARGE_PAYLOAD_RANGE[1])];
		}
		random2.nextBytes(payload);
		totalBytes += payload.length;
		return new String(payload, StandardCharsets.UTF_8);
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
