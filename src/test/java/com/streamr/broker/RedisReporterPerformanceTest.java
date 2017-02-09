package com.streamr.broker;

import com.streamr.broker.redis.RedisReporter;

class RedisReporterPerformanceTest {

	private static int NUM_OF_MESSAGES = 500000;
	private static int PAYLOAD_SIZE_IN_BYTES = 8192;

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(10000, 1);
		brokerProcess.setUpConsumer(new RedisReporter("127.0.0.1", "kakka"));
		brokerProcess.setUpProducer(queueProducer -> () -> {
			RandomStreamrBinaryMessageGenerator generator = new RandomStreamrBinaryMessageGenerator();
			for (int i = 0; i < NUM_OF_MESSAGES; ++i) {
				queueProducer.accept(generator.provideMessage(PAYLOAD_SIZE_IN_BYTES, i));
			}
		});
		brokerProcess.startAll();
	}
}
