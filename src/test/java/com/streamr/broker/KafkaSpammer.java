package com.streamr.broker;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class KafkaSpammer {
	private final KafkaProducer<String, byte[]> producer;
	private final String dataTopic;

	public static void main(String[] args) {
		KafkaSpammer kafkaSpammer = new KafkaSpammer("127.0.0.1:9092", "data-dev");
		Random random = new Random();

		int count = 0;
		while (true) {
			byte[] paylod = new byte[2048];
			random.nextBytes(paylod);
			kafkaSpammer.send("jf98sajgdsajglkdgjds9ag9", paylod);
			if (++count % 10000 == 0) {
				System.out.println(count);
			}
		}
	}

	public KafkaSpammer(String kafkaHost, String dataTopic) {
		this.producer = new KafkaProducer<>(makeKafkaConfig(kafkaHost));
		this.dataTopic = dataTopic;
	}

	public void send(String streamId, byte[] payload) {
		int partition = 0;
		String kafkaPartitionKey = streamId +  "-" + partition;

		StreamrBinaryMessage msg = new StreamrBinaryMessage(
			streamId,
			partition,
			System.currentTimeMillis(),
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			payload
		);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(dataTopic, kafkaPartitionKey, msg.toBytes());
		producer.send(record);
	}

	private static Properties makeKafkaConfig(String zookeeperHost) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", zookeeperHost);
		properties.setProperty("producer.type", "async");
		properties.setProperty("queue.buffering.max.ms", "100");
		properties.setProperty("retry.backoff.ms", "500");
		properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("request.required.acks", "0");
		return properties;
	}
}
