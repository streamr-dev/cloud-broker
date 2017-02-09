package com.streamr.broker;

import com.streamr.broker.reporter.Reporter;

import java.util.concurrent.*;
import java.util.function.Function;

public class BrokerProcess {
	private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "consumer"));
	private final ExecutorService producerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "producer"));
	private final ScheduledExecutorService statsExecutor = Executors.newScheduledThreadPool(1,
		r -> new Thread(r, "status"));

	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final Stats stats;
	private Runnable consumer;
	private Runnable producer;

	public BrokerProcess(int queueSize, int statsInterval) {
		queue = new ArrayBlockingQueue<>(queueSize);
		stats = new Stats(statsInterval);
	}

	public void setUpConsumer(Reporter... reporters) {
		consumer = new QueueConsumer(queue, reporters);
	}

	public void setUpProducer(Function<QueueProducer, Runnable> cb) {
		producer = cb.apply(new QueueProducer(queue, stats));
	}

	public void startAll() {
		startProducer();
		startConsumer();
		startStatsLogging();
	}

	public void startConsumer() {
		consumerExecutor.submit(consumer);
	}

	public void startProducer() {
		producerExecutor.submit(producer);
	}

	public void startStatsLogging() {
		statsExecutor.scheduleAtFixedRate(stats, stats.getStatsIntervalSecs(), stats.getStatsIntervalSecs(),
			TimeUnit.SECONDS);
	}

	public Stats getStats() {
		return stats;
	}
}
