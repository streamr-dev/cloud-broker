package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraBatchReporter;
import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.redis.RedisReporter;
import com.streamr.broker.stats.LoggedStats;

public class Main {

	private static LoggedStats stats;

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(Config.QUEUE_SIZE);
		stats = new LoggedStats();
		brokerProcess.setStats(stats, Config.STATS_INTERVAL_IN_SECS);
		brokerProcess.setUpProducer((queueProducer ->
			new KafkaListener(Config.KAFKA_HOST, Config.KAFKA_GROUP, Config.KAFKA_TOPIC, queueProducer)));
		brokerProcess.setUpConsumer(
			new RedisReporter(Config.REDIS_HOST, Config.REDIS_PORT, Config.REDIS_PASSWORD),
			new CassandraBatchReporter(Config.getCassandraHosts(), Config.CASSANDRA_KEYSPACE, Config.CASSANDRA_USERNAME, Config.CASSANDRA_PASSWORD),
			new TestNetReporter(Config.getNodes())
		);
		brokerProcess.startAll();
	}

	public static LoggedStats getStats() {
		return stats;
	}
}
