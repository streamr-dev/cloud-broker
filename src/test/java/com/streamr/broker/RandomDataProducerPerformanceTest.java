package com.streamr.broker;

class RandomDataProducerPerformanceTest {

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(PerformanceTestConfiguration.QUEUE_SIZE);
		brokerProcess.setStats(new MeanStats(), 3);
		brokerProcess.setUpConsumer();
		brokerProcess.setUpProducer(new RandomDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
