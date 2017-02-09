package com.streamr.broker.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.streamr.broker.stats.Stats;
import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import com.streamr.broker.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Semaphore;

public class CassandraReporter implements Reporter {
	private static final Logger log = LogManager.getLogger();

	private final Session session;
	private final CassandraStatementBuilder cassandraStatementBuilder;
	private final Semaphore semaphore;
	private final Stats stats;

	public CassandraReporter(String cassandraHost, String cassandraKeySpace, Stats stats) {
		Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();
		session = cluster.connect(cassandraKeySpace);
		cassandraStatementBuilder = new CassandraStatementBuilder(session);
		semaphore = new Semaphore(cluster.getConfiguration().getPoolingOptions().getMaxQueueSize(), true);
		log.info("Cassandra session created for {} on key space '{}'", cluster.getMetadata().getAllHosts(),
			session.getLoggedKeyspace());
		this.stats = stats;
	}

	@Override
	public void report(StreamrBinaryMessageWithKafkaMetadata msg) {
		BoundStatement eventPs = cassandraStatementBuilder.eventInsert(msg);
		BoundStatement tsPs = cassandraStatementBuilder.tsInsert(msg);
		semaphore.acquireUninterruptibly();
		ResultSetFuture f1 = session.executeAsync(eventPs);
		ResultSetFuture f2 = session.executeAsync(tsPs);
		Futures.addCallback(Futures.allAsList(f1, f2), new FutureCallback<List<ResultSet>>() {
			@Override
			public void onSuccess(List<ResultSet> result) {
				semaphore.release();
				stats.onWrittenToCassandra(msg);
			}

			@Override
			public void onFailure(Throwable t) {
				semaphore.release();
				throw new RuntimeException(t);
			}
		});
	}

	@Override
	public void close() {
		session.getCluster().close();
	}
}
