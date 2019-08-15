package com.streamr.broker;

import com.streamr.broker.stats.EventsStats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.io.Closeable;

public interface Reporter extends Closeable {
	void setStats(EventsStats stats);

	default void setStats(EventsStats[] stats) {
		for (EventsStats s: stats) {
			setStats(s);
		}
	}

	void report(StreamMessage msg);

	@Override
	void close();
}
