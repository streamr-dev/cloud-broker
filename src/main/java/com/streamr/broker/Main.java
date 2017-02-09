package com.streamr.broker;

import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.reporter.CassandraReporter;
import com.streamr.broker.reporter.RedisReporter;

import java.util.concurrent.*;

public class Main {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		String zookeeper = System.getProperty("kafka.server", "127.0.0.1:9092");
		String kafkaGroup = System.getProperty("kafka.group", "data-dev");
		String kafkaTopic = System.getProperty("kafka.topic", "data-dev");
		String redisHost = System.getProperty("redis.host", "127.0.0.1");
		String redisPassword = System.getProperty("redis.password", "kakka");
		String cassandraHost = System.getProperty("cassandra.host", "127.0.0.1");
		String cassandraKeySpace = System.getProperty("cassandra.keyspace", "streamr_dev");
		int queueSize = Integer.parseInt(System.getProperty("queuesize", "2000"));
		int statsInterval = Integer.parseInt(System.getProperty("statsinterval", "3"));

		BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue = new ArrayBlockingQueue<>(queueSize);
		Stats stats = new Stats(statsInterval);

		KafkaListener producer = new KafkaListener(zookeeper, kafkaGroup, kafkaTopic, new QueueProducer(queue, stats));
		QueueConsumer consumer = new QueueConsumer(queue,
			new RedisReporter(redisHost, redisPassword),
			new CassandraReporter(cassandraHost, cassandraKeySpace, stats)
		);

		ExecutorService producerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "producer"));
		ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "consumer"));
		producerExecutor.submit(producer);
		consumerExecutor.submit(consumer);

		ScheduledExecutorService statusExecutor = Executors.newScheduledThreadPool(1, r -> new Thread(r, "status"));
		statusExecutor.scheduleAtFixedRate(stats::report, 1, statsInterval, TimeUnit.SECONDS);
	}
}
