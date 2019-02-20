package com.streamr.broker.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.streamr.broker.Config
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.protocol.message_layer.StreamMessageV28
import com.streamr.client.protocol.message_layer.StreamMessageV29
import com.streamr.client.protocol.message_layer.StreamMessageV30
import groovy.transform.CompileStatic
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer

class CassandraStatementBuilderSpec extends Specification {
	@Shared
	Cluster cluster = Cluster.builder().addContactPoint(Config.CASSANDRA_HOST).build()

	Session session
	CassandraStatementBuilder builder

	void setup() {
		session = cluster.connect(Config.CASSANDRA_KEYSPACE)
		clearData(session)
		builder = new CassandraStatementBuilder(session)
	}

	void cleanup() {
		clearData(session)
		session?.close()
	}

	void cleanupSpec() {
		cluster?.close()
	}

	void "eventInsert() inserts expected data (v28) to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamMessageV28(
				"cassandraStatementBuilderSpec-streamId",
				0,
				timestamp,
				0,
				51248,
				51247,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"hello": "world!"}',
		)

		when:
		def statement = builder.eventInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
				"SELECT * FROM stream_data WHERE id = ? ALLOW FILTERING",
				"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("id") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("partition") == 0
		rs.first().getTimestamp("ts") == new Date(timestamp)
		rs.first().getLong("sequence_no") == 0
		rs.first().getString("publisher_id") == ""
		rs.first().getBytes("payload") == ByteBuffer.wrap(message.toBytes())
	}

	void "eventInsert() inserts expected data (v29) to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamMessageV29(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			51248,
			51247,
			StreamMessage.ContentType.CONTENT_TYPE_JSON,
			'{"hello": "world!"}',
			StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b"
		)

		when:
		def statement = builder.eventInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
			"SELECT * FROM stream_data WHERE id = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("id") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("partition") == 0
		rs.first().getTimestamp("ts") == new Date(timestamp)
		rs.first().getLong("sequence_no") == 0
		rs.first().getString("publisher_id") == "0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4"
		rs.first().getBytes("payload") == ByteBuffer.wrap(message.toBytes())
	}

	void "eventInsert() inserts expected data (v30) to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamMessageV30(
				"cassandraStatementBuilderSpec-streamId",
				0,
				timestamp,
				0,
				"publisherId",
				"msgChainId",
				timestamp - 500,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"hello": "world!"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b"
		)

		when:
		def statement = builder.eventInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
				"SELECT * FROM stream_data WHERE id = ? ALLOW FILTERING",
				"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("id") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("partition") == 0
		rs.first().getTimestamp("ts") == new Date(timestamp)
		rs.first().getLong("sequence_no") == 0
		rs.first().getString("publisher_id") == "publisherId"
		rs.first().getString("msg_chain_id") == "msgChainId"
		rs.first().getBytes("payload") == ByteBuffer.wrap(message.toBytes())
	}

	void "eventInsertBatch() inserts expected data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def msg1 = new StreamMessageV30(
				"cassandraStatementBuilderSpec-streamId",
				0,
				timestamp,
				0,
				"publisherId",
				"msgChainId",
				(Long) null,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"hello": "world!"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b"
		)
		def msg2 = new StreamMessageV30(
				"cassandraStatementBuilderSpec-streamId",
				0,
				timestamp + 500,
				0,
				"publisherId",
				"msgChainId",
				timestamp,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"hello": "world!"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b"
		)
		def msg3 = new StreamMessageV30(
				"cassandraStatementBuilderSpec-streamId",
				0,
				timestamp + 1000,
				0,
				"publisherId",
				"msgChainId",
				timestamp + 500,
				0,
				StreamMessage.ContentType.CONTENT_TYPE_JSON,
				'{"hello": "world!"}',
				StreamMessage.SignatureType.SIGNATURE_TYPE_ETH,
				"0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b"
		)

		when:
		session.execute(builder.eventBatchInsert([msg1, msg2, msg3]))

		then:
		def rs = session.execute(
			"SELECT * FROM stream_data WHERE id = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 3
	}

	@CompileStatic
	private static void clearData(Session session) {
		session.execute("DELETE FROM stream_data WHERE id = ? AND partition = ?",
			"cassandraStatementBuilderSpec-streamId", 0)
	}
}
