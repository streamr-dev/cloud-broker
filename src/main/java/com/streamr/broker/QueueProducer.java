package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueProducer implements Consumer<StreamMessage> {
	private final BlockingQueue<StreamMessage> queue;
	private final Stats stats;

	public QueueProducer(BlockingQueue<StreamMessage> queue, Stats stats) {
		this.queue = queue;
		this.stats = stats;
	}

	@Override
	public void accept(StreamMessage msg) {
		try {
			queue.put(msg);
			stats.onReadFromKafka(msg);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
