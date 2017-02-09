package com.streamr.broker;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueProducer implements Consumer<StreamrBinaryMessageWithKafkaMetadata> {
	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final Stats stats;

	public QueueProducer(BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue, Stats stats) {
		this.queue = queue;
		this.stats = stats;
	}

	@Override
	public void accept(StreamrBinaryMessageWithKafkaMetadata msg) {
		try {
			queue.put(msg);
			stats.onReadFromKafka(msg);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
