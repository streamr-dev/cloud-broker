package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.function.Function;

public class BrokerProcess {
	private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "consumer"));
	private final ExecutorService producerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "producer"));
	private final ScheduledExecutorService statsExecutor = Executors.newScheduledThreadPool(1,
		r -> new Thread(r, "statsLogger"));

	private final BlockingQueue<StreamMessage> queue;
	private Stats[] stats;
	private Runnable consumer;
	private Runnable producer;

	public BrokerProcess(int queueSize) {
		this.queue = new ArrayBlockingQueue<>(queueSize);
	}

	public void setStats(Stats[] stats) {
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
		for(Stats s: stats) {
			s.start();
			statsExecutor.scheduleAtFixedRate(s::report, s.getIntervalInSec(), s.getIntervalInSec(), TimeUnit.SECONDS);
		}
	}

	public void shutdown() {
		producerExecutor.shutdownNow(); // todo: wait for empty
		consumerExecutor.shutdownNow();
		statsExecutor.shutdownNow();
		for(Stats s: stats) {
			s.stop();
		}
	}
}
