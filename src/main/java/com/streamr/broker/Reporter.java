package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;

import java.io.Closeable;
import java.util.ArrayList;

public interface Reporter extends Closeable {
	void setStats(Stats stats);

	default void setStats(ArrayList<Stats> stats) {
		for (Stats s: stats) {
			setStats(s);
		}
	}

	void report(StreamMessage msg);

	@Override
	void close();
}
