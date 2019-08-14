package com.streamr.broker.stats;

import com.streamr.client.protocol.message_layer.StreamMessage;

public interface Stats {
	void onReadFromKafka(StreamMessage msg);
	void onWrittenToCassandra(StreamMessage msg);
	void onWrittenToRedis(StreamMessage msg);
	void onCassandraWriteError();
	int getIntervalInSec();
	void start();
	void stop();
	void report();

	void setReservedMessageSemaphores(int reservedMessageSemaphores);
	void setReservedCassandraSemaphores(int reservedCassandraSemaphores);
}
