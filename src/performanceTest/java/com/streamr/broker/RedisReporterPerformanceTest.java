package com.streamr.broker;

import com.streamr.broker.redis.RedisReporter;

/**
 #semaphores    time    throughput       throughput     memory use                  cpu use
 without        28s     235673 kb/s      63481 #/s      962MB -> 1400MB             30%
 16      		44s     176000 kb/s      44600 #/s      268MB -> 200MB -> 145MB     25%
 64      		35s     215000 kb/s      55000 #/s      523MB -> 273MB -> 128MB     25%
 256     		30s     258000 kb/s      65000 #/s      669MB -> 400MB -> 100MB     28%
 1024    		37s     201000 kb/s      52300 #/s      862MB -> 1100MB -> 600MB    25%
 8196    		30s     253000 kb/s      64000 #/s      1026MB -> 1400MB            30%
 */
class RedisReporterPerformanceTest {

	public static void main(String[] args) {
		MeanStats meanStats = new MeanStats();
		BrokerProcess brokerProcess = new BrokerProcess(PerformanceTestConfiguration.QUEUE_SIZE);
		brokerProcess.setStats(meanStats, 3);
		brokerProcess.setUpConsumer(new RedisReporter("127.0.0.1", ""));
		brokerProcess.setUpProducer(new RandomDataProducer().producer(brokerProcess));
		brokerProcess.startAll();
	}
}
