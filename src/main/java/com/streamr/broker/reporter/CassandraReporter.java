package com.streamr.broker.reporter;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CassandraReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private static final int COMMIT_INTERVAL_MS = 1000;

	private final Map<String, Batch> batches = new ConcurrentHashMap<>();
	private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	private final Session session;
	private final CassandraStatementBuilder cassandraStatementBuilder;

	public CassandraReporter(String cassandraHost, String cassandraKeySpace) {
		Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();
		session = cluster.connect(cassandraKeySpace);
		cassandraStatementBuilder = new CassandraStatementBuilder(session);
		log.info("Cassandra session created for {} on key space '{}'", cluster.getMetadata().getAllHosts(),
			session.getLoggedKeyspace());
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		String key = formKey(msg);
		Batch batch = batches.get(key);
		if (batch == null) {
			batch = new Batch(key);
			batches.put(key, batch);
			scheduledExecutor.schedule(batch, COMMIT_INTERVAL_MS, TimeUnit.MILLISECONDS);
		}
		batch.add(msg);
	}

	@Override
	public void close() {
		session.getCluster().close();
	}

	private static String formKey(StreamrBinaryMessageWithKafkaMetadata msg) {
		return msg.getStreamId() + "|" + msg.getPartition();
	}

	private class Batch extends TimerTask {
		private final String key;
		private final List<StreamrBinaryMessageWithKafkaMetadata> messages = new ArrayList<>();

		Batch(String key) {
			this.key = key;
		}

		void add(StreamrBinaryMessageWithKafkaMetadata msg) {
			messages.add(msg);
		}

		@Override
		public void run() {
			batches.remove(key);
			long startTime = System.currentTimeMillis();
			BatchStatement eventPs = cassandraStatementBuilder.eventBatchInsert(messages);
			BatchStatement tsPs = cassandraStatementBuilder.tsBatchInsert(messages);
			session.executeAsync(eventPs);
			session.executeAsync(tsPs);
			log.debug("Wrote data for {} into Cassandra. {} messages, {} bytes, and {} seconds.", key,
				messages.size(), totalSizeInBytes(), (System.currentTimeMillis() - startTime) / 1000.0);
		}

		private long totalSizeInBytes() {
			long total = 0;
			for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
				total += msg.toBytes().length;
			}
			return total;
		}
	}
}
