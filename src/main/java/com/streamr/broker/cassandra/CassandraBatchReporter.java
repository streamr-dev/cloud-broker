package com.streamr.broker.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.streamr.broker.Reporter;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import com.streamr.broker.stats.Stats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CassandraBatchReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private static final int COMMIT_INTERVAL_MS = 1000;
	private static final int DO_NOT_GROW_BATCH_AFTER_BYTES = 5000;

	private final Map<String, Batch> batches = new HashMap<>();
	private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	private final Session session;
	private final CassandraStatementBuilder cassandraStatementBuilder;
	private final Semaphore semaphore;
	private Stats stats;

	public CassandraBatchReporter(String cassandraHost, String cassandraKeySpace) {
		Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();
		session = cluster.connect(cassandraKeySpace);
		cassandraStatementBuilder = new CassandraStatementBuilder(session);
		semaphore = new Semaphore(cluster.getConfiguration().getPoolingOptions().getMaxQueueSize(), true);
		log.info("Cassandra session created for {} on keyspace '{}'", cluster.getMetadata().getAllHosts(),
			session.getLoggedKeyspace());
	}

	@Override
	public void setStats(Stats stats) {
		this.stats = stats;
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		String key = formKey(msg);
		synchronized (batches) {
			Batch batch = batches.get(key);
			if (batch == null || batch.isFull()) {
				batch = new Batch(key);
				batches.put(key, batch);
				scheduledExecutor.schedule(batch, COMMIT_INTERVAL_MS, TimeUnit.MILLISECONDS);
			}
			batch.add(msg);
		}
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
		private long totalSizeInBytes = 0;
		private final List<StreamrBinaryMessageWithKafkaMetadata> messages = new ArrayList<>();
		private final FutureCallback<List<ResultSet>> statsCallback = new FutureCallback<List<ResultSet>>() {
			@Override
			public void onSuccess(List<ResultSet> result) {
				semaphore.release();
				for (StreamrBinaryMessageWithKafkaMetadata msg : messages) {
					stats.onWrittenToCassandra(msg);
				}
			}

			@Override
			public void onFailure(Throwable t) {
				semaphore.release();
				throw new RuntimeException(t);
			}
		};

		Batch(String key) {
			this.key = key;
		}

		boolean isFull() {
			return totalSizeInBytes >= DO_NOT_GROW_BATCH_AFTER_BYTES;
		}

		void add(StreamrBinaryMessageWithKafkaMetadata msg) {
			totalSizeInBytes += msg.sizeInBytes();
			messages.add(msg);
		}

		@Override
		public void run() {
			synchronized (batches) {
				batches.remove(key, this);
			}
			semaphore.acquireUninterruptibly();
			BatchStatement eventPs = cassandraStatementBuilder.eventBatchInsert(messages);
			BatchStatement tsPs = cassandraStatementBuilder.tsBatchInsert(messages);
			ResultSetFuture f1 = session.executeAsync(eventPs);
			ResultSetFuture f2 = session.executeAsync(tsPs);
			Futures.addCallback(Futures.allAsList(f1, f2), statsCallback);
		}
	}
}
