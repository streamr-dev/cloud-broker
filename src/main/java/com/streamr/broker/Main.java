package com.streamr.broker;

import com.streamr.broker.reporter.CassandraReporter;
import com.streamr.broker.reporter.RedisReporter;

public class Main {
	public static void main(String[] args) {
		String zookeeper = System.getProperty("kafka.server", "dev.streamr:9092");
		String kafkaGroup = System.getProperty("kafka.group", "streamr");
		String kafkaTopic = System.getProperty("kafka.topic", "streamr_dev");
		String redisUrl = System.getProperty("redis.url", "redis://dev.streamr");
		String redisPassword = System.getProperty("redis.password", "AFuPxeVMwBKHV5Hm5SK3PkRZA");
		String cassandraHost = System.getProperty("cassandra.host", "dev.streamr");
		String cassandraKeySpace = System.getProperty("cassandra.keyspace", "streamr_dev");

		KafkaRecordHandler kafkaRecordHandler = new KafkaRecordHandler(
			new RedisReporter(redisUrl, redisPassword),
			new CassandraReporter(cassandraHost, cassandraKeySpace));

		KafkaListener kafkaListener = new KafkaListener(zookeeper, kafkaGroup);
		kafkaListener.subscribeAndListen(kafkaTopic, kafkaRecordHandler);
	}
}
