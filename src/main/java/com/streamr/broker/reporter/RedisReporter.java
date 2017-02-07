package com.streamr.broker.reporter;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.pubsub.api.sync.RedisPubSubCommands;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RedisReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();
	private final RedisClient client;
	private final RedisPubSubCommands<byte[], byte[]> connection;

	public RedisReporter(String url, String password) {
		RedisURI uri = RedisURI.create(url);
		uri.setPassword(password);
		client = RedisClient.create(url);
		connection = client.connectPubSub(new ByteArrayCodec()).sync(); // TODO: async version?
		log.info("Connection created for " + uri);
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		connection.publish(formKey(msg), msg.toBytesWithKafkaMetadata());
	}

	@Override
	public void close() throws IOException {
		connection.close();
		client.shutdown();
	}

	private static byte[] formKey(StreamrBinaryMessageWithKafkaMetadata msg) {
		String s = msg.getStreamId() + "-" + msg.getPartition();
		return s.getBytes(StandardCharsets.UTF_8);
	}
}
