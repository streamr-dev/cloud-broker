package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraBatchReporter;
import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.redis.RedisReporter;
import com.streamr.broker.stats.LoggedStats;

import java.util.concurrent.ExecutionException;

public class Main {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		BrokerProcess brokerProcess = new BrokerProcess(Config.QUEUE_SIZE);
		brokerProcess.setStats(new LoggedStats(), Config.STATS_INTERVAL_IN_SECS);
		brokerProcess.setUpProducer((queueProducer ->
			new KafkaListener(Config.KAFKA_HOST, Config.KAFKA_GROUP, Config.KAFKA_TOPIC, queueProducer)));
		brokerProcess.setUpConsumer(
			new RedisReporter(Config.REDIS_HOST, Config.REDIS_PASSWORD),
			new CassandraBatchReporter(Config.CASSANDRA_HOST, Config.CASSANDRA_KEYSPACE)
		);
		brokerProcess.startAll();
	}
}
