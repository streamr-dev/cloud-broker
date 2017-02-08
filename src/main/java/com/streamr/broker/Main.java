package com.streamr.broker;

import com.streamr.broker.reporter.CassandraReporter;
import com.streamr.broker.reporter.RedisReporter;

public class Main {
	public static void main(String[] args) {
		String zookeeper = System.getProperty("kafka.server", "127.0.0.1:9092");
		String kafkaGroup = System.getProperty("kafka.group", "data-dev");
		String kafkaTopic = System.getProperty("kafka.topic", "data-dev");
		String redisHost = System.getProperty("redis.host", "127.0.0.1");
		String redisPassword = System.getProperty("redis.password", "kakka");
		String cassandraHost = System.getProperty("cassandra.host", "127.0.0.1");
		String cassandraKeySpace = System.getProperty("cassandra.keyspace", "streamr_dev");

		KafkaRecordHandler kafkaRecordHandler = new KafkaRecordHandler(
			new RedisReporter(redisHost, redisPassword),
			new CassandraReporter(cassandraHost, cassandraKeySpace));

		KafkaListener kafkaListener = new KafkaListener(zookeeper, kafkaGroup);
		kafkaListener.subscribeAndListen(kafkaTopic, kafkaRecordHandler);
	}
}
