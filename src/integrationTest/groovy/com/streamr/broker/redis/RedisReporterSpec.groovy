package com.streamr.broker.redis

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.api.sync.RedisCommands
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.lambdaworks.redis.pubsub.RedisPubSubListener
import com.lambdaworks.redis.pubsub.api.sync.RedisPubSubCommands
import com.streamr.broker.Config
import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
import com.streamr.broker.stats.LoggedStats
import com.streamr.broker.stats.Stats
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable

class RedisReporterSpec extends Specification {
	StreamrBinaryMessageWithKafkaMetadata testMessage = new StreamrBinaryMessageWithKafkaMetadata(
		"streamId",
		0,
		System.currentTimeMillis(),
		0,
		StreamrBinaryMessage.CONTENT_TYPE_STRING,
		"hello world".bytes,
		1,
		10561,
		10560
	)

	RedisReporter reporter = new RedisReporter(Config.REDIS_HOST, Config.REDIS_PASSWORD)
	RedisClient client

	void setup() {
		reporter.setStats(new LoggedStats())
		RedisURI uri = RedisURI.Builder.redis(Config.REDIS_HOST).withPassword(Config.REDIS_PASSWORD).build()
		client = RedisClient.create(uri)
	}

	void cleanup() {
		client.shutdown()
		reporter.close()
	}

	void "report() publishes to Redis pub-sub"() {
		def blockingVariable = new BlockingVariable<byte[]>(5)

		RedisPubSubCommands<byte[], byte[]> commands = client.connectPubSub(new ByteArrayCodec()).sync()
		commands.addListener(new RedisPubSubListener<byte[], byte[]>() {
			@Override
			void message(byte[] channel, byte[] message) {
				blockingVariable.set(message)
			}

			@Override
			void message(byte[] pattern, byte[] channel, byte[] message) {}

			@Override
			void subscribed(byte[] channel, long count) {}

			@Override
			void psubscribed(byte[] pattern, long count) {}

			@Override
			void unsubscribed(byte[] channel, long count) {}

			@Override
			void punsubscribed(byte[] pattern, long count) {}
		})
		byte[] channel = "streamId-0".bytes
		commands.subscribe(channel, channel)

		when:
		reporter.report(testMessage)
		then:
		blockingVariable.get() == testMessage.toBytesWithKafkaMetadata()

		cleanup:
		commands.close()
	}

	void "report() sets expiring key-value pair"() {
		RedisCommands<String, String> commands = client.connect().sync()

		when:
		reporter.report(testMessage)
		then:
		commands.get("streamId-0") == "10561"
		commands.ttl("streamId-0") <= 5

		cleanup:
		commands.close()
	}

	void "report() invokes Stats#onWrittenToRedis(msg)"() {
		def blockingVariable = new BlockingVariable<StreamrBinaryMessageWithKafkaMetadata>(5)
		reporter.setStats(new Stats() {
			@Override
			void onReadFromKafka(StreamrBinaryMessageWithKafkaMetadata msg) {}

			@Override
			void onWrittenToCassandra(StreamrBinaryMessageWithKafkaMetadata msg) {}

			@Override
			void onWrittenToRedis(StreamrBinaryMessageWithKafkaMetadata msg) {
				blockingVariable.set(msg)
			}

			@Override
			void start(int intervalInSec) {}

			@Override
			void stop() {}

			@Override
			void report() {}

			@Override
			void onCassandraWriteError() {}
		})

		when:
		reporter.report(testMessage)

		then:
		blockingVariable.get() == testMessage
	}
}
