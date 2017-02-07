package com.streamr.broker;

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

class KafkaListener {
	private static final Logger log = LogManager.getLogger();
	private final Consumer<String, byte[]> consumer;

	KafkaListener(String zookeeperHost, String groupId) {
		consumer = new KafkaConsumer<>(makeKafkaConfig(zookeeperHost, groupId));
		log.info("Consumer created (host: '{}', groupId: '{}')", zookeeperHost, groupId);
	}

	void subscribeAndListen(String dataTopic, java.util.function.Consumer<ConsumerRecord<String, byte[]>> recordHandler) {
		consumer.subscribe(Collections.singletonList(dataTopic));
		log.info("Subscribed to data topic '{}'", dataTopic);
		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				for (ConsumerRecord<String, byte[]> record : records) {
					recordHandler.accept(record);
				}
			}
		} finally {
			consumer.close();
			// TODO: invoke close() of reporters indirectly
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
		// TODO: optimize settings for performance
		return properties;
	}
}
