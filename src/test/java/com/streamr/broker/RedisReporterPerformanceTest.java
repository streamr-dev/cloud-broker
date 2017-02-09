package com.streamr.broker;

import com.streamr.broker.redis.RedisReporter;

class RedisReporterPerformanceTest {

	public static void main(String[] args) {
		MeanStats meanStats = new MeanStats();
		BrokerProcess brokerProcess = new BrokerProcess(PerformanceConfig.QUEUE_SIZE);
		brokerProcess.setStats(meanStats, 3);
		brokerProcess.setUpConsumer(new RedisReporter("127.0.0.1", "kakka"));
		brokerProcess.setUpProducer(new FakeDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
