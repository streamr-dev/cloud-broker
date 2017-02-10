package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraBatchReporter;
import com.streamr.broker.cassandra.CassandraReporter;

import static com.streamr.broker.PerformanceTestConfiguration.QUEUE_SIZE;

class CassandraReporterPerformanceTest {
	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(QUEUE_SIZE);
		brokerProcess.setStats(new MeanStats(), 3);
		brokerProcess.setUpConsumer(new CassandraBatchReporter("127.0.0.1", "streamr_dev"));
		brokerProcess.setUpProducer(new RandomDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
