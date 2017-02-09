package com.streamr.broker;

import com.streamr.broker.stats.LoggedStats;

import java.util.concurrent.*;
import java.util.function.Function;

public class BrokerProcess {
	private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "consumer"));
	private final ExecutorService producerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "producer"));
	private final ScheduledExecutorService statsExecutor = Executors.newScheduledThreadPool(1,
		r -> new Thread(r, "statsLogger"));

	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final LoggedStats stats;
	private Runnable consumer;
	private Runnable producer;

	public BrokerProcess(int queueSize, int statsInterval) {
		queue = new ArrayBlockingQueue<>(queueSize);
		stats = new LoggedStats(statsInterval);
	}


	public void setUpProducer(Function<QueueProducer, Runnable> cb) {
		producer = cb.apply(new QueueProducer(queue, stats));
	}

	public void setUpConsumer(Reporter... reporters) {
		for (Reporter reporter : reporters) {
			reporter.setStats(stats);
		}
		consumer = new QueueConsumer(queue, reporters);
	}

	public void startAll() {
		startProducer();
		startConsumer();
		startStatsLogging();
	}

	public void startProducer() {
		producerExecutor.submit(producer);
	}

	public void startConsumer() {
		consumerExecutor.submit(consumer);
	}

	public void startStatsLogging() {
		statsExecutor.scheduleAtFixedRate(stats, stats.getIntervalInSec(), stats.getIntervalInSec(), TimeUnit.SECONDS);
	}
}
