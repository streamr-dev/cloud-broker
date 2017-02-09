package com.streamr.broker;

class GenerationPerformanceTest {

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(PerformanceConfig.QUEUE_SIZE);
		brokerProcess.setStats(new MeanStats(), 3);
		brokerProcess.setUpConsumer();
		brokerProcess.setUpProducer(new FakeDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
