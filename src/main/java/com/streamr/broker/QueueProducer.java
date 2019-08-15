package com.streamr.broker;

import com.streamr.broker.stats.EventsStats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueProducer implements Consumer<StreamMessage> {
	private final BlockingQueue<StreamMessage> queue;
	private final EventsStats[] stats;

	public QueueProducer(BlockingQueue<StreamMessage> queue, EventsStats[] stats) {
		this.queue = queue;
		this.stats = stats;
	}

	@Override
	public void accept(StreamMessage msg) {
		try {
			queue.put(msg);
			for (int i = 0; i < stats.length; i++) {
				stats[i].onReadFromKafka(msg);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
