package com.streamr.broker;

import com.streamr.broker.stats.Stats;

import java.util.concurrent.*;
import java.util.function.Function;

public class BrokerProcess {
	private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "consumer"));
	private final ExecutorService producerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "producer"));
	private final ScheduledExecutorService statsExecutor = Executors.newScheduledThreadPool(1,
		r -> new Thread(r, "statsLogger"));

	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final int intervalInSec;
	private Stats stats;
	private Runnable consumer;
	private Runnable producer;

	public BrokerProcess(int queueSize, int intervalInSec) {
		this.queue = new ArrayBlockingQueue<>(queueSize);
		this.intervalInSec = intervalInSec;
	}

	public void setStats(Stats stats) {
		this.stats = stats;
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
		startStatsLogging();
		startProducer();
		startConsumer();
	}

	public void startProducer() {
		producerExecutor.submit(producer);
	}

	public void startConsumer() {
		consumerExecutor.submit(consumer);
	}

	public void startStatsLogging() {
		stats.start(intervalInSec);
		statsExecutor.scheduleAtFixedRate(stats::report, intervalInSec, intervalInSec, TimeUnit.SECONDS);
	}

	public void kill() {
		producerExecutor.shutdownNow();
		consumerExecutor.shutdownNow();
		statsExecutor.shutdownNow();
	}
}
