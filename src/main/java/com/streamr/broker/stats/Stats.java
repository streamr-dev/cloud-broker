package com.streamr.broker.stats;

import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;

public interface Stats {
	void onReadFromKafka(StreamrBinaryMessageWithKafkaMetadata msg);
	void onWrittenToCassandra(StreamrBinaryMessageWithKafkaMetadata msg);
	void onWrittenToRedis(StreamrBinaryMessageWithKafkaMetadata msg);
	void start(int intervalInSec);
	void stop();
	void report();
}
