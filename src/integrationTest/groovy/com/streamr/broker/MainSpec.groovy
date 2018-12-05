package com.streamr.broker

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.lambdaworks.redis.pubsub.api.sync.RedisPubSubCommands
import groovy.transform.CompileStatic
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.ByteBuffer

class MainSpec extends Specification {

	@Shared
	KafkaDataProducer dataProducer = new KafkaDataProducer(Config.KAFKA_HOST, Config.KAFKA_TOPIC)

	@Shared
	RedisClient redisClient = RedisClient.create(
		RedisURI.Builder.redis(Config.REDIS_HOST)
			.withPassword(Config.REDIS_PASSWORD)
			.build()
	)

	@Shared
	Cluster cassandraCluster = Cluster.builder()
		.addContactPoint(Config.CASSANDRA_HOST)
		.build()

	@Shared
	String streamId = "MainSpec" + System.currentTimeMillis()

	void cleanupSpec() {
		dataProducer?.close()
		redisClient?.shutdown()
		cassandraCluster?.close()
	}

	void "forwards new Kafka data to Cassandra and Redis (end-to-end test)"() {
		List<byte[]> receivedMessages = []
		RedisPubSubCommands<byte[], byte[]> redisPubSub = redisClient.connectPubSub(new ByteArrayCodec()).sync()
		redisPubSub.addListener(new TestRedisListener(receivedMessages))
		redisPubSub.subscribe("${streamId}-1".bytes, "${streamId}-1".bytes)

		Main.main(new String[0])

		when: "500 new data points arrive to Kafka"
		(1..500).each {
			dataProducer.produceToKafka(new StreamrBinaryMessageV28(
				streamId,
				1,
				System.currentTimeMillis() + (it * 10000),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_STRING,
				"I am message number ${it}".bytes)
			)
		}

		then: "all data points are published into Redis Pub/Sub"
		new PollingConditions(timeout: 5).eventually {
			assert receivedMessages.size() == 500
		}

		then: "last offset is stored in Redis Key-Value"
		getRedisKeyValue("${streamId}-1") == getOffsetOfMessage(receivedMessages.last())

		and: "all data points are stored into Cassandra"
		new PollingConditions(timeout: 10, initialDelay: 1, delay: 0.5).eventually {
			assert fetchFromCassandra(streamId).size() == 500
			assert fetchFromCassandraTimestamps(streamId).size() == 500
		}

		cleanup:
		redisPubSub.close()
	}

	@CompileStatic
	private String getRedisKeyValue(String key) {
		redisClient.connect().sync().withCloseable {
			it.get(key)
		}
	}

	@CompileStatic
	String getOffsetOfMessage(byte[] bytes) {
		new StreamrBinaryMessageWithKafkaMetadata(ByteBuffer.wrap(bytes)).offset
	}

	@CompileStatic
	private ResultSet fetchFromCassandra(String streamId) {
		cassandraCluster.connect(Config.CASSANDRA_KEYSPACE).withCloseable {
			it.execute("SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING", streamId)
		}
	}

	@CompileStatic
	private ResultSet fetchFromCassandraTimestamps(String streamId) {
		cassandraCluster.connect(Config.CASSANDRA_KEYSPACE).withCloseable {
			it.execute("SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING", streamId)
		}
	}
}
