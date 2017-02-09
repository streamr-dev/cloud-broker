package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraReporter;

import static com.streamr.broker.PerformanceConfig.QUEUE_SIZE;

class CassandraReporterPerformanceTest {
	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(QUEUE_SIZE);
		brokerProcess.setStats(new MeanStats(), 3);
		brokerProcess.setUpConsumer(new CassandraReporter("127.0.0.1", "streamr_dev"));
		brokerProcess.setUpProducer(new FakeDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
