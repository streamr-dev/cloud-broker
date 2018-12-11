package com.streamr.broker;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.streamr.broker.PerformanceTestConfiguration.NUM_OF_MESSAGES;

public class KafkaSpammer {
	private final RandomDataProducer randomDataProducer = new RandomDataProducer();
	private final KafkaProducer<String, byte[]> producer;
	private final String dataTopic;

	public KafkaSpammer(String kafkaHost, String dataTopic) {
		this.producer = new KafkaProducer<>(makeKafkaConfig(kafkaHost));
		this.dataTopic = dataTopic;
	}

	public void spam() {
		for (int i=0; i < NUM_OF_MESSAGES; ++i) {
			sendRandomDataToKafka();
		}
		randomDataProducer.logTotalByesGenerated();
		producer.close();
	}

	private void sendRandomDataToKafka() {
		StreamrBinaryMessageWithKafkaMetadata msg = randomDataProducer.provideMessage(0);
		String kafkaPartitionKey = msg.getStreamrBinaryMessage().getStreamId() +  "-" + msg.getStreamrBinaryMessage().getPartition();
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(dataTopic, kafkaPartitionKey, msg.getStreamrBinaryMessage().toBytes());
		producer.send(record);
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
