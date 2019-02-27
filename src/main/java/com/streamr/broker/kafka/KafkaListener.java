package com.streamr.broker.kafka;

import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class KafkaListener implements Runnable {
	private static final Logger log = LogManager.getLogger();

	private final KafkaRecordTransformer kafkaRecordTransformer = new KafkaRecordTransformer();
	private final java.util.function.Consumer<StreamMessage> callback;
	private final Consumer<String, byte[]> consumer;

	public KafkaListener(String zookeeperHost, String groupId, String dataTopic,
						 java.util.function.Consumer<StreamMessage> callback) {
		consumer = new KafkaConsumer<>(makeKafkaConfig(zookeeperHost, groupId));
		log.info("Kafka consumer created for '{}' in consumer group '{}'", zookeeperHost, groupId);
		consumer.subscribe(Collections.singletonList(dataTopic));
		log.info("Subscribed to data topic '{}'", dataTopic);
		this.callback = callback;
	}

	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(Long.MAX_VALUE); // wait indefinitely
			for (ConsumerRecord<String, byte[]> record : records) {
				try {
					callback.accept(kafkaRecordTransformer.transform(record));
				} catch (Throwable e) {
					log.throwing(e);
				}
			}
		}
	}

	private static Properties makeKafkaConfig(String zookeeperHost, String groupId) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", zookeeperHost);
		properties.setProperty("group.id", groupId);
		properties.setProperty("session.timeout.ms", "15000");
		properties.setProperty("auto.offset.reset", "latest");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
		return properties;
	}
}
