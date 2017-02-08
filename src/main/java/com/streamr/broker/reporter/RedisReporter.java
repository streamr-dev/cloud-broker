package com.streamr.broker.reporter;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.pubsub.api.async.RedisPubSubAsyncCommands;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class RedisReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private final RedisClient client;
	private final RedisPubSubAsyncCommands<byte[], byte[]> connection;

	public RedisReporter(String host, String password) {
		RedisURI uri = RedisURI.Builder.redis(host).withPassword(password).build();
		client = RedisClient.create(uri);
		connection = client.connectPubSub(new ByteArrayCodec()).async();
		log.info("Redis connection created for " + uri);
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		connection.publish(formKey(msg), msg.toBytesWithKafkaMetadata());
	}

	@Override
	public void close() {
		connection.close();
		client.shutdown();
	}

	private static byte[] formKey(StreamrBinaryMessageWithKafkaMetadata msg) {
		String s = msg.getStreamId() + "-" + msg.getPartition();
		return s.getBytes(StandardCharsets.UTF_8);
	}
}
