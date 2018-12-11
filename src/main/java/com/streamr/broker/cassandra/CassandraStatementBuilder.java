package com.streamr.broker.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

class CassandraStatementBuilder {
	private final PreparedStatement eventInsertPs;
	private final PreparedStatement eventInsertTtlPs;
	private final PreparedStatement tsInsertPs;
	private final PreparedStatement tsInsertTtlPs;

	CassandraStatementBuilder(Session session) {
		eventInsertPs = session.prepare("INSERT INTO stream_events" +
			" (stream, stream_partition, kafka_partition, kafka_offset, previous_offset, ts, payload)" +
			" VALUES (?, ?, ?, ?, ?, ?, ?)");
		eventInsertTtlPs = session.prepare("INSERT INTO stream_events" +
			"(stream, stream_partition, kafka_partition, kafka_offset, previous_offset, ts, payload)" +
			" VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?");
		tsInsertPs = session.prepare("INSERT INTO stream_timestamps" +
			" (stream, stream_partition, kafka_offset, ts)" +
			" VALUES (?, ?, ?, ?)");
		tsInsertTtlPs = session.prepare("INSERT INTO stream_timestamps" +
			" (stream, stream_partition, kafka_offset, ts)" +
			" VALUES (?, ?, ?, ?) USING TTL ?");
	}

	BoundStatement eventInsert(StreamrBinaryMessageWithKafkaMetadata msg) {
		if (msg.getStreamrBinaryMessage().getTTL() > 0) {
			return eventInsertTtlPs.bind(
				msg.getStreamrBinaryMessage().getStreamId(),
				msg.getStreamrBinaryMessage().getPartition(),
				msg.getKafkaPartition(),
				msg.getOffset(),
				msg.getPreviousOffset(),
				new Date(msg.getStreamrBinaryMessage().getTimestamp()),
				ByteBuffer.wrap(msg.getStreamrBinaryMessage().toBytes()),
				msg.getStreamrBinaryMessage().getTTL());
		} else {
			return eventInsertPs.bind(
				msg.getStreamrBinaryMessage().getStreamId(),
				msg.getStreamrBinaryMessage().getPartition(),
				msg.getKafkaPartition(),
				msg.getOffset(),
				msg.getPreviousOffset(),
				new Date(msg.getStreamrBinaryMessage().getTimestamp()),
				ByteBuffer.wrap(msg.getStreamrBinaryMessage().toBytes()));
		}
	}


	BoundStatement tsInsert(StreamrBinaryMessageWithKafkaMetadata msg) {
		if (msg.getStreamrBinaryMessage().getTTL() > 0) {
			return tsInsertTtlPs.bind(
				msg.getStreamrBinaryMessage().getStreamId(),
				msg.getStreamrBinaryMessage().getPartition(),
				msg.getOffset(),
				new Date(msg.getStreamrBinaryMessage().getTimestamp()),
				msg.getStreamrBinaryMessage().getTTL()
			);
		} else {
			return tsInsertPs.bind(
				msg.getStreamrBinaryMessage().getStreamId(),
				msg.getStreamrBinaryMessage().getPartition(),
				msg.getOffset(),
				new Date(msg.getStreamrBinaryMessage().getTimestamp())
			);
		}
	}

	BatchStatement eventBatchInsert(List<StreamrBinaryMessageWithKafkaMetadata> messages) {
		BatchStatement batchStatement = new BatchStatement();
		for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
			batchStatement.add(eventInsert(msg));
		}
		return batchStatement;
	}

	BatchStatement tsBatchInsert(List<StreamrBinaryMessageWithKafkaMetadata> messages) {
		BatchStatement batchStatement = new BatchStatement();

		// Avoid writing sub-second timestamps for same key
		long lastWrittenTimestamp = -1001;
		for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
			if (msg.getStreamrBinaryMessage().getTimestamp() - lastWrittenTimestamp > 1000) {
				lastWrittenTimestamp = msg.getStreamrBinaryMessage().getTimestamp();
				batchStatement.add(tsInsert(msg));
			}
		}

		return batchStatement;
	}
}
