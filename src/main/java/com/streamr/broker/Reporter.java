package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.io.Closeable;

public interface Reporter extends Closeable {
	void setStats(Stats stats);

	void report(StreamMessage msg);

	@Override
	void close();
}
