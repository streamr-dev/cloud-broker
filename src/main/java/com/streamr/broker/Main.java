package com.streamr.broker;

import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.reporter.CassandraReporter;
import com.streamr.broker.reporter.RedisReporter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
	public static void main(String[] args) {
		String zookeeper = System.getProperty("kafka.server", "127.0.0.1:9092");
		String kafkaGroup = System.getProperty("kafka.group", "data-dev");
		String kafkaTopic = System.getProperty("kafka.topic", "data-dev");
		String redisHost = System.getProperty("redis.host", "127.0.0.1");
		String redisPassword = System.getProperty("redis.password", "kakka");
		String cassandraHost = System.getProperty("cassandra.host", "127.0.0.1");
		String cassandraKeySpace = System.getProperty("cassandra.keyspace", "streamr_dev");

		BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue = new ArrayBlockingQueue<>(500);

		KafkaListener producer = new KafkaListener(zookeeper, kafkaGroup, kafkaTopic, new QueueProducer(queue));
		QueueConsumer consumer = new QueueConsumer(queue,
			new RedisReporter(redisHost, redisPassword),
			new CassandraReporter(cassandraHost, cassandraKeySpace)
		);

		// TODO: how to get ExecutorService to actually work -.-
		//ExecutorService producerExecutor = Executors.newSingleThreadExecutor();
		//producerExecutor.submit(producer);
		new Thread(producer, "queueProducer").start();
		new Thread(consumer, "queueConsumer").start();
		//ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
		//consumerExecutor.submit(consumer);
	}
}
