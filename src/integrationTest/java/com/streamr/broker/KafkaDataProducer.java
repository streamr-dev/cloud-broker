package com.streamr.broker;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

/**
 * Utility for producing data to Apache Kafka
 */
public class KafkaDataProducer implements Closeable {
	private static final Logger log = LogManager.getLogger();

	private final KafkaProducer<String, byte[]> producer;
	private final String dataTopic;

	public KafkaDataProducer(String kafkaHost, String dataTopic) {
		this.producer = new KafkaProducer<>(makeKafkaConfig(kafkaHost));
		this.dataTopic = dataTopic;
		log.info("Kafka producer created for '{}'", kafkaHost);
	}

	public void produceToKafka(StreamrBinaryMessage msg) {
		String kafkaPartitionKey = msg.getStreamId() +  "-" + msg.getPartition();
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(dataTopic, kafkaPartitionKey, msg.toBytes());
		producer.send(record);
		log.info("Produced to '{}'", dataTopic);
	}

	@Override
	public void close() throws IOException {
		producer.close();
		log.info("Closed Kafka producer");
	}

	private static Properties makeKafkaConfig(String zookeeperHost) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", zookeeperHost);
		properties.setProperty("retry.backoff.ms", "500");
		properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		return properties;
	}
}
