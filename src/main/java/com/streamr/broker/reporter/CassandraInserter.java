package com.streamr.broker.reporter;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

class CassandraInserter {
	private static final Logger log = LogManager.getLogger();

	private final PreparedStatement eventInsertPs;
	private final PreparedStatement eventInsertTtlPs;
	private final PreparedStatement tsInsertPs;
	private final PreparedStatement tsInsertTtlPs;
	private final Session session;

	CassandraInserter(Session session) {
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
		this.session = session;
	}

	void insert(List<StreamrBinaryMessageWithKafkaMetadata> messages) {
		BatchStatement eventInsertionBatch = new BatchStatement();
		BatchStatement tsInsertionBatch = new BatchStatement();

		for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
			if (msg.getTTL() > 0) {
				eventInsertionBatch.add(eventInsertTtlPs.bind(
					msg.getStreamId(),
					msg.getPartition(),
					msg.getKafkaPartition(),
					msg.getOffset(),
					msg.getPreviousOffset(),
					new Date(msg.getTimestamp()),
					ByteBuffer.wrap(msg.toBytes()),
					msg.getTTL()));
			} else {
				eventInsertionBatch.add(eventInsertPs.bind(
					msg.getStreamId(),
					msg.getPartition(),
					msg.getKafkaPartition(),
					msg.getOffset(),
					msg.getPreviousOffset(),
					new Date(msg.getTimestamp()),
					ByteBuffer.wrap(msg.toBytes())));
			}
		}

		// Avoid writing sub-second timestamps for same key
		long lastWrittenTimestamp = -1001;
		for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
			if (msg.getTimestamp() - lastWrittenTimestamp > 1000) {
				lastWrittenTimestamp = msg.getTimestamp();
				if (msg.getTTL() > 0) {
					tsInsertionBatch.add(tsInsertTtlPs.bind(
						msg.getStreamId(),
						msg.getPartition(),
						msg.getOffset(),
						new Date(msg.getTimestamp()),
						msg.getTTL()
					));
				} else {
					tsInsertionBatch.add(tsInsertPs.bind(
						msg.getStreamId(),
						msg.getPartition(),
						msg.getOffset(),
						new Date(msg.getTimestamp())
					));
				}
			}
		}

		session.execute(eventInsertionBatch); // TODO: async
		session.execute(tsInsertionBatch);
	}
}
