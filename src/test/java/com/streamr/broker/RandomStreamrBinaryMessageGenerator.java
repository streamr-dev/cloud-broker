package com.streamr.broker;

import java.util.Random;

class RandomStreamrBinaryMessageGenerator {
	private final Random random = new Random();

	StreamrBinaryMessageWithKafkaMetadata provideMessage(int payloadSize, long offset) {
		return new StreamrBinaryMessageWithKafkaMetadata("streamId",
			1,
			14351238112L,
			1000,
			StreamrBinaryMessageWithKafkaMetadata.CONTENT_TYPE_STRING,
			randomBytes(payloadSize),
			0,
			offset,
			offset - 1);
	}

	private byte[] randomBytes(int length) {
		byte[] bytes = new byte[length];
		random.nextBytes(bytes);
		return bytes;
	}
}
