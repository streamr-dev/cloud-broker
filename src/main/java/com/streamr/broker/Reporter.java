package com.streamr.broker;

import com.streamr.broker.stats.Stats;

import java.io.Closeable;

public interface Reporter extends Closeable {
	void setStats(Stats stats);

	void report(StreamrBinaryMessageWithKafkaMetadata msg);

	@Override
	void close();
}
