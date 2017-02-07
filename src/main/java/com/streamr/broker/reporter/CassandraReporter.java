package com.streamr.broker.reporter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class CassandraReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private static final int COMMIT_INTERVAL_MS = 1000;
	private static final int MAX_BUNDLE = 500;

	private final Map<String, Batch> batches = new HashMap<>();
	private final Timer timer = new Timer();
	private final Session session;
	private final CassandraInserter cassandraInserter;

	public CassandraReporter(String cassandraHost, String cassandraKeySpace) {
		Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();
		session = cluster.connect(cassandraKeySpace);
		cassandraInserter = new CassandraInserter(session);
		log.info("Created session for {} on key space '{}'",
			cluster.getMetadata().getAllHosts(),
			session.getLoggedKeyspace());
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		String key = formKey(msg);
		Batch batch = batches.get(key);
		if (batch == null) {
			batch = new Batch(key);
			batches.put(key, batch);
			timer.schedule(batch, COMMIT_INTERVAL_MS);
		}
		batch.add(msg);
	}

	@Override
	public void close() throws IOException {
		session.getCluster().close();
	}

	private static String formKey(StreamrBinaryMessageWithKafkaMetadata msg) {
		return msg.getStreamId() + "/" + msg.getPartition();
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
			cassandraInserter.insert(messages);
			batches.remove(key);
		}
	}
}
