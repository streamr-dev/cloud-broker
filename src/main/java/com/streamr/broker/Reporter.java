package com.streamr.broker;

import java.io.Closeable;

public interface Reporter extends Closeable {
	void report(StreamrBinaryMessageWithKafkaMetadata msg);

	@Override
	void close();
}
