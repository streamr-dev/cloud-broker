package com.streamr.broker;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueProducer implements Consumer<StreamrBinaryMessageWithKafkaMetadata> {
	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;

	QueueProducer(BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue) {
		this.queue = queue;
	}

	@Override
	public void accept(StreamrBinaryMessageWithKafkaMetadata msg) {
		try {
			queue.put(msg);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
