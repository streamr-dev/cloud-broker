package com.streamr.broker.kafka;

import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
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
	private final java.util.function.Consumer<StreamrBinaryMessageWithKafkaMetadata> callback;
	private final Consumer<String, byte[]> consumer;

	public KafkaListener(String zookeeperHost, String groupId, String dataTopic,
						 java.util.function.Consumer<StreamrBinaryMessageWithKafkaMetadata> callback) {
		consumer = new KafkaConsumer<>(makeKafkaConfig(zookeeperHost, groupId));
		log.info("Kafka consumer created for '{}' on group '{}')", zookeeperHost, groupId);
		consumer.subscribe(Collections.singletonList(dataTopic));
		log.info("Subscribed to data topic '{}'", dataTopic);
		this.callback = callback;
	}

	@Override
	public void run() {
		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				for (ConsumerRecord<String, byte[]> record : records) {
					callback.accept(kafkaRecordTransformer.transform(record));
				}
			}
		} catch (Exception e) {
			log.throwing(e);
		} finally {
			log.info("Aborting...");
			consumer.close();
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
