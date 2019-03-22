package com.streamr.broker

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.lambdaworks.redis.pubsub.api.sync.RedisPubSubCommands
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV30
import groovy.transform.CompileStatic
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

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
	Cluster cassandraCluster

	@Shared
	String streamId = "MainSpec" + System.currentTimeMillis()

	void setupSpec() {
		Cluster.Builder builder = Cluster.builder()
		Config.getCassandraHosts().each { builder.addContactPoint(it) }
		cassandraCluster = builder.build()
	}

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
		(1..100).each {
			dataProducer.produceToKafka(new StreamrBinaryMessageV28(
				streamId,
				1,
				System.currentTimeMillis() + (it * 10000),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				('{"I am message number": "'+it+'"}').bytes)
			)
		}
		(101..200).each {
			dataProducer.produceToKafka(new StreamrBinaryMessageV29(
				streamId,
				1,
				System.currentTimeMillis() + (it * 10000),
				0,
				StreamrBinaryMessage.CONTENT_TYPE_JSON,
				('{"I am message number": "'+it+'"}').bytes,
				StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b")
			)
		}
		(201..500).each {
			dataProducer.produceToKafka(new StreamMessageV30(
				streamId,
				1,
				System.currentTimeMillis() + (it * 10000),
				0,
				"publisherId",
				"msgChainId",
				null,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"I am message number": "' + it + '"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b")
			)
		}

		then: "all data points are published into Redis Pub/Sub"
		new PollingConditions(timeout: 5).eventually {
			assert receivedMessages.size() == 500
		}

		and: "all data points are stored into Cassandra"
		new PollingConditions(timeout: 10, initialDelay: 1, delay: 0.5).eventually {
			assert fetchFromCassandra(streamId).size() == 500
		}

		cleanup:
		redisPubSub.close()
	}

	@CompileStatic
	private ResultSet fetchFromCassandra(String streamId) {
		cassandraCluster.connect(Config.CASSANDRA_KEYSPACE).withCloseable {
			it.execute("SELECT * FROM stream_data WHERE id = ? ALLOW FILTERING", streamId)
		}
	}
}
