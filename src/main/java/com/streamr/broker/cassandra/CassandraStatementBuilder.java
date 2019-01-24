package com.streamr.broker.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.nio.ByteBuffer;
import java.util.List;

class CassandraStatementBuilder {
	private final PreparedStatement eventInsert;

	CassandraStatementBuilder(Session session) {
		eventInsert = session.prepare("INSERT INTO stream_data" +
			" (id, partition, ts, sequence_no, publisher_id, payload)" +
			" VALUES (?, ?, ?, ?, ?, ?)");
	}

	BoundStatement eventInsert(StreamMessage msg) {
		return eventInsert.bind(
			msg.getStreamId(),
			msg.getStreamPartition(),
			msg.getTimestampAsDate(),
			(int) msg.getSequenceNumber(),
			msg.getPublisherId(),
			ByteBuffer.wrap(msg.toBytes()));
	}

	BatchStatement eventBatchInsert(List<StreamMessage> messages) {
		BatchStatement batchStatement = new BatchStatement();
		for (StreamMessage msg : messages) {
			batchStatement.add(eventInsert(msg));
		}
		return batchStatement;
	}
}
