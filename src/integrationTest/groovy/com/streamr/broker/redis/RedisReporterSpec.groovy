package com.streamr.broker.redis

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.lambdaworks.redis.pubsub.RedisPubSubListener
import com.lambdaworks.redis.pubsub.api.sync.RedisPubSubCommands
import com.streamr.broker.Config
import com.streamr.broker.stats.LoggedStats
import com.streamr.broker.stats.Stats
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV30
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable

class RedisReporterSpec extends Specification {
	StreamMessage testMessage = new StreamMessageV30(
			"streamId",
			0,
			System.currentTimeMillis(),
			0,
			"publisherId",
			"msgChainId",
			System.currentTimeMillis() - 1000,
			0,
			StreamMessage.ContentType.CONTENT_TYPE_JSON,
			'{"hello": "world"}',
			StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
			"signature"
	)

	RedisReporter reporter = new RedisReporter(Config.REDIS_HOST, Config.REDIS_PORT, Config.REDIS_PASSWORD)
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
		blockingVariable.get() == testMessage.toBytes()

		cleanup:
		commands.close()
	}

	void "report() invokes Stats#onWrittenToRedis(msg)"() {
		def blockingVariable = new BlockingVariable<StreamMessage>(5)
		reporter.setStats(new Stats() {
			@Override
			void onReadFromKafka(StreamMessage msg) {}

			@Override
			void onWrittenToCassandra(StreamMessage msg) {}

			@Override
			void onWrittenToRedis(StreamMessage msg) {
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

			@Override
			void setReservedMessageSemaphores(int reservedMessageSemaphores) {}

			@Override
			void setReservedCassandraSemaphores(int reservedCassandraSemaphores) {}
		})

		when:
		reporter.report(testMessage)

		then:
		blockingVariable.get() == testMessage
	}
}
