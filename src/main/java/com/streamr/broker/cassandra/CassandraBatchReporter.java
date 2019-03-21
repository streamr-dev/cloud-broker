package com.streamr.broker.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.streamr.broker.Reporter;
import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CassandraBatchReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private static final int BASE_COMMIT_INTERVAL_IN_MS = 1000;
	private static final int MAX_FAIL_MULTIPLIER = 64;
	private static final int DO_NOT_GROW_BATCH_AFTER_BYTES = 1024 * 1024 * 2; // optimized
	private static final int MAX_MESSAGES_IN_MEMORY = 65536;

	private final Map<String, Batch> batches = new HashMap<>();
	private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	private final Session session;
	private final CassandraStatementBuilder cassandraStatementBuilder;
	private final Semaphore numOfMessagesSemaphore; // Ensure heap doesn't run out from too many messages
	private final Semaphore cassandraSemaphore;     // Ensure Cassandra doesn't explode from too many async queries
	private Stats stats;
	private int failMultiplier = 1;

	public CassandraBatchReporter(String[] cassandraHosts, String cassandraKeySpace, String username, String password) {
		Cluster cluster = null;
		try {
			Cluster.Builder builder = Cluster.builder().withCredentials(username, password)
					.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE));
			for(String host: cassandraHosts) {
				builder = builder.addContactPoint(host);
			}
			cluster = builder.build();
			session = cluster.connect(cassandraKeySpace);
			cassandraSemaphore = new Semaphore(cluster.getConfiguration().getPoolingOptions().getMaxQueueSize(), true);
			cassandraStatementBuilder = new CassandraStatementBuilder(session);
			numOfMessagesSemaphore = new Semaphore(MAX_MESSAGES_IN_MEMORY);
			log.info("Cassandra session created for {} on keyspace '{}'", cluster.getMetadata().getAllHosts(),
				session.getLoggedKeyspace());
		} catch (Exception e) {
			scheduledExecutor.shutdownNow();
			if (cluster != null) {
				cluster.close(); // => session.close()
			}
			throw e;
		}
	}

	@Override
	public void setStats(Stats stats) {
		this.stats = stats;
	}

	@Override
	public void report(StreamMessage msg) {
		numOfMessagesSemaphore.acquireUninterruptibly();
		String key = formKey(msg);
		synchronized (batches) {
			Batch batch = batches.get(key);
			if (batch == null || batch.isFull()) {
				batch = new Batch(key);
				batches.put(key, batch);
				scheduledExecutor.schedule(batch, getCommitIntervalInMs(), TimeUnit.MILLISECONDS);
			}
			batch.add(msg);
		}
	}

	@Override
	public void close() {
		scheduledExecutor.shutdownNow();
		session.getCluster().close(); // => session.close()
	}

	private static String formKey(StreamMessage msg) {
		return msg.getStreamId() + "|" + msg.getStreamPartition();
	}

	private int getCommitIntervalInMs() {
		return BASE_COMMIT_INTERVAL_IN_MS * failMultiplier;
	}

	private void growFailMultiplier() {
		int candidate = failMultiplier * 2;
		if (candidate <= MAX_FAIL_MULTIPLIER) {
			failMultiplier = candidate;
		}
	}

	private class Batch extends TimerTask {
		private final String key;
		private long totalSizeInBytes = 0;
		private final List<StreamMessage> messages = new ArrayList<>();
		private final FutureCallback<ResultSet> statsCallback = new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet result) {
				failMultiplier = 1;
				numOfMessagesSemaphore.release(messages.size());
				cassandraSemaphore.release();
				log.info("Available permits in: numOfMessagesSemaphore: {}, cassandraSemaphore: {}",
						numOfMessagesSemaphore.availablePermits(), cassandraSemaphore.availablePermits());
				for (StreamMessage msg : messages) {
					stats.onWrittenToCassandra(msg);
				}
			}

			@Override
			public void onFailure(Throwable t) {
				cassandraSemaphore.release();
				growFailMultiplier();
				long commitIntervalInMs = getCommitIntervalInMs();
				StreamMessage firstMessage = messages.get(0);
				StreamMessage lastMessage = messages.get(messages.size() - 1);
				log.error("Failed to write to '{}'. Timestamps {} - {}. Total bytes {}. Exception: {}." +
						"Re-scheduled to {} ms.",
					firstMessage.getStreamId(),
					firstMessage.getTimestamp(),
					lastMessage.getTimestamp(),
					totalSizeInBytes,
					t,
					commitIntervalInMs
				);
				stats.onCassandraWriteError();
				scheduledExecutor.schedule(Batch.this, commitIntervalInMs, TimeUnit.MILLISECONDS);
			}
		};

		Batch(String key) {
			this.key = key;
		}

		boolean isFull() {
			return totalSizeInBytes >= DO_NOT_GROW_BATCH_AFTER_BYTES;
		}

		void add(StreamMessage msg) {
			totalSizeInBytes += msg.sizeInBytes();
			messages.add(msg);
		}

		@Override
		public void run() {
			synchronized (batches) {
				batches.remove(key, this);
			}
			BatchStatement eventPs = cassandraStatementBuilder.eventBatchInsert(messages);
			cassandraSemaphore.acquireUninterruptibly();
			ResultSetFuture f = session.executeAsync(eventPs);
			Futures.addCallback(f, statsCallback);
		}
	}
}
