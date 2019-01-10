package com.streamr.broker.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.streamr.broker.Config
import com.streamr.broker.StreamrBinaryMessage
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata
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

	void "eventInsert() inserts expected data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world!".bytes,
			3,
			51248,
			51247
		)

		when:
		def statement = builder.eventInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("stream") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("stream_partition") == 0
		rs.first().getInt("kafka_partition") == 3
		rs.first().getLong("kafka_offset") == 51248
		rs.first().getLong("previous_offset") == 51247
		rs.first().getTimestamp("ts") == new Date(timestamp)
		rs.first().getBytes("payload") == ByteBuffer.wrap(message.toBytesWithKafkaMetadata())
	}

	void "eventInsert() with TTL inserts disappearing data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world!".bytes,
			3,
			51249,
			51248
		)

		when:
		def statement = builder.eventInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("stream") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("stream_partition") == 0
		rs.first().getInt("kafka_partition") == 3
		rs.first().getLong("kafka_offset") == 51249
		rs.first().getLong("previous_offset") == 51248
		rs.first().getTimestamp("ts") == new Date(timestamp)
		rs.first().getBytes("payload") == ByteBuffer.wrap(message.toBytesWithKafkaMetadata())


		when:
		sleep(1000)
		def rs2 = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		then:
		rs2.size() == 0

	}

	void "tsInsert() inserts expected data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world!".bytes,
			3,
			51248,
			51247
		)

		when:
		def statement = builder.tsInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
			"SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("stream") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("stream_partition") == 0
		rs.first().getLong("kafka_offset") == 51248
		rs.first().getTimestamp("ts") == new Date(timestamp)
	}

	void "tsInsert() with TTL inserts disappearing data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def message = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world!".bytes,
			3,
			51249,
			51248
		)

		when:
		def statement = builder.tsInsert(message)
		session.execute(statement)

		then:
		def rs = session.execute(
			"SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
		rs.first().getString("stream") == "cassandraStatementBuilderSpec-streamId"
		rs.first().getInt("stream_partition") == 0
		rs.first().getLong("kafka_offset") == 51249
		rs.first().getTimestamp("ts") == new Date(timestamp)

		when:
		sleep(1000)
		def rs2 = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		then:
		rs2.size() == 0

	}

	void "eventInsertBatch() inserts expected (disappearing and persistent) data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def msg1 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #1!".bytes,
			3,
			51250,
			51247
		)
		def msg2 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp + 60000,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #2!".bytes,
			3,
			51255,
			51250
		)
		def msg3 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp+ 120000,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #3!".bytes,
			3,
			51258,
			51255
		)

		when:
		session.execute(builder.eventBatchInsert([msg1, msg2, msg3]))

		then:
		def rs = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 3

		when:
		sleep(1000)
		def rs2 = session.execute(
			"SELECT * FROM stream_events WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		then:
		rs2.size() == 2
	}

	void "tsBatchInsert() inserts expected (disappearing and persistent) data to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def msg1 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #1!".bytes,
			3,
			51250,
			51247
		)
		def msg2 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp + 60000,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #2!".bytes,
			3,
			51255,
			51250
		)
		def msg3 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp+ 120000,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #3!".bytes,
			3,
			51258,
			51255
		)

		when:
		session.execute(builder.tsBatchInsert([msg1, msg2, msg3]))

		then:
		def rs = session.execute(
			"SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 3

		when:
		sleep(1000)
		def rs2 = session.execute(
			"SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		then:
		rs2.size() == 2
	}

	void "tsBatchInsert() avoids writing sub-second entries to Cassandra"() {
		def timestamp = System.currentTimeMillis()
		def msg1 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #1!".bytes,
			3,
			51250,
			51247
		)
		def msg2 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp + 500,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #2!".bytes,
			3,
			51255,
			51250
		)
		def msg3 = new StreamrBinaryMessageWithKafkaMetadata(
			"cassandraStatementBuilderSpec-streamId",
			0,
			timestamp + 999,
			0,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"hello, world #3!".bytes,
			3,
			51258,
			51255
		)

		when:
		session.execute(builder.tsBatchInsert([msg1, msg2, msg3]))

		then:
		def rs = session.execute(
			"SELECT * FROM stream_timestamps WHERE stream = ? ALLOW FILTERING",
			"cassandraStatementBuilderSpec-streamId"
		).all()

		rs.size() == 1
	}

	@CompileStatic
	private static void clearData(Session session) {
		session.execute("DELETE FROM stream_events WHERE stream = ? AND stream_partition = ?",
			"cassandraStatementBuilderSpec-streamId", 0)
		session.execute("DELETE FROM stream_timestamps WHERE stream = ? AND stream_partition = ?",
			"cassandraStatementBuilderSpec-streamId", 0)
	}
}
