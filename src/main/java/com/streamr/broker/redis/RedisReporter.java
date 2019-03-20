package com.streamr.broker.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.pubsub.api.async.RedisPubSubAsyncCommands;
import com.streamr.broker.Reporter;
import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class RedisReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private final RedisClient client;
	private final RedisPubSubAsyncCommands<byte[], byte[]> pubSub;
	private final RedisAsyncCommands<String, String> commands;
	private Stats stats;

	public RedisReporter(String host, int port, String password) {
		RedisURI uri = RedisURI.Builder.redis(host).withPort(port).withPassword(password).build();
		client = RedisClient.create(uri);
		pubSub = client.connectPubSub(new ByteArrayCodec()).async();
		commands = client.connect().async();
		log.info("Redis connection created for " + uri);
	}

	@Override
	public void setStats(Stats stats) {
		this.stats = stats;
	}

	@Override
	public void report(StreamMessage msg) {
		String key = formKey(msg);
		pubSub.publish(key.getBytes(StandardCharsets.UTF_8), msg.toBytes())
				.thenRunAsync(() -> stats.onWrittenToRedis(msg));
	}

	@Override
	public void close() {
		pubSub.close();
		commands.close();
		client.shutdown();
	}

	private static String formKey(StreamMessage msg) {
		return msg.getStreamId() + "-" + msg.getStreamPartition();
	}
}
