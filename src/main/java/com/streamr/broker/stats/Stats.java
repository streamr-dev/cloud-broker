package com.streamr.broker.stats;

import com.streamr.client.protocol.message_layer.StreamMessage;

public interface Stats {
	void onReadFromKafka(StreamMessage msg);
	void onReportedToCassandra(StreamMessage msg);
	void onWrittenToCassandra(StreamMessage msg);
	void onWrittenToRedis(StreamMessage msg);
	void onCassandraWriteError();
	void start(int intervalInSec);
	void stop();
	void report();

	void setReservedMessageSemaphores(int reservedMessageSemaphores);
	void setReservedCassandraSemaphores(int reservedCassandraSemaphores);
}
