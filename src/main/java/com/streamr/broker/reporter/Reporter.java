package com.streamr.broker.reporter;

import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;

import java.io.Closeable;

public interface Reporter extends Closeable {
	void report(StreamrBinaryMessageWithKafkaMetadata msg);
}
