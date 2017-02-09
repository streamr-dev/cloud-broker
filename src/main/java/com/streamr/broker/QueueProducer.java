package com.streamr.broker;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueProducer implements Consumer<StreamrBinaryMessageWithKafkaMetadata> {
	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final Stats stats;

	QueueProducer(BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue, Stats stats) {
		this.queue = queue;
		this.stats = stats;
	}

	@Override
	public void accept(StreamrBinaryMessageWithKafkaMetadata msg) {
		try {
			queue.put(msg);
			stats.onMessageProduced(msg);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
