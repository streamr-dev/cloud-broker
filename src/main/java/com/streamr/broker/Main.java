package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraBatchReporter;
import com.streamr.broker.cassandra.CassandraReporter;
import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.redis.RedisReporter;
import com.streamr.broker.stats.LoggedStats;

import java.util.concurrent.ExecutionException;

public class Main {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		String zookeeper = System.getProperty("kafka.server", "127.0.0.1:9092");
		String kafkaGroup = System.getProperty("kafka.group", "data-dev");
		String kafkaTopic = System.getProperty("kafka.topic", "data-dev");
		String redisHost = System.getProperty("redis.host", "127.0.0.1");
		String redisPassword = System.getProperty("redis.password", "kakka");
		String cassandraHost = System.getProperty("cassandra.host", "127.0.0.1");
		String cassandraKeySpace = System.getProperty("cassandra.keyspace", "streamr_dev");
		int queueSize = Integer.parseInt(System.getProperty("queuesize", "200"));
		int statsIntervalInSec = Integer.parseInt(System.getProperty("statsinterval", "30"));

		BrokerProcess brokerProcess = new BrokerProcess(queueSize);
		brokerProcess.setStats(new LoggedStats(), statsIntervalInSec);
		brokerProcess.setUpProducer((queueProducer ->
			new KafkaListener(zookeeper, kafkaGroup, kafkaTopic, queueProducer)));
		brokerProcess.setUpConsumer(
			new RedisReporter(redisHost, redisPassword),
			new CassandraBatchReporter(cassandraHost, cassandraKeySpace)
		);
		brokerProcess.startAll();
	}
}
