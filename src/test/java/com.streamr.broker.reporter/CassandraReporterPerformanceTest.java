package com.streamr.broker.reporter;

import com.streamr.broker.BrokerProcess;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import com.streamr.broker.cassandra.CassandraReporter;

import java.util.Random;

class CassandraReporterPerformanceTest {

	private static int NUM_OF_MESSAGES = 500000;
	private static int PAYLOAD_SIZE_IN_BYTES = 8192;

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(5000, 1);
		brokerProcess.setUpConsumer(new CassandraReporter("127.0.0.1", "streamr_dev",
			brokerProcess.getStats())
		);
		brokerProcess.setUpProducer(queueProducer -> () -> {
			for (int i=0; i < NUM_OF_MESSAGES; ++i) {
				queueProducer.accept(provideMessage(i));
			}
		});
		brokerProcess.startAll();
	}

	private static StreamrBinaryMessageWithKafkaMetadata provideMessage(long offset) {
		return new StreamrBinaryMessageWithKafkaMetadata("streamId",
			1,
			14351238112L,
			1000,
			StreamrBinaryMessageWithKafkaMetadata.CONTENT_TYPE_STRING,
			randomBytes(PAYLOAD_SIZE_IN_BYTES),
			0,
			offset,
			offset - 1);
	}

	private static final Random random = new Random();
	private static byte[] randomBytes(int length) {
		byte[] bytes = new byte[length];
		random.nextBytes(bytes);
		return bytes;
	}
}
