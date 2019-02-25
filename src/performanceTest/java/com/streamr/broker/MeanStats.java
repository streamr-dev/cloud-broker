package com.streamr.broker;

import com.streamr.broker.stats.Stats;
import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class MeanStats implements Stats {
	private static final Logger log = LogManager.getLogger();

	private long totalBytesRead = 0;
	private int numOfEventsRead = 0;
	private long totalBytesWrittenToCassandra = 0;
	private int totalEventsWrittenToCassandra = 0;
	private long totalBytesWrittenToRedis = 0;
	private int totalEventsWrittenToRedis = 0;

	private long startTime;

	@Override
	public void start(int intervalInSec) {
		startTime = System.currentTimeMillis();
	}

	@Override
	public void stop() {
		report();
		log.info("Finished in {} seconds.", (System.currentTimeMillis() - startTime) / 1000);
	}

	@Override
	public void onReadFromKafka(StreamMessage msg) {
		totalBytesRead += msg.sizeInBytes();
		numOfEventsRead++;
	}

	@Override
	public void onWrittenToCassandra(StreamMessage msg) {
		totalBytesWrittenToCassandra += msg.sizeInBytes();
		totalEventsWrittenToCassandra++;
	}

	@Override
	public void onWrittenToRedis(StreamMessage msg) {
		totalBytesWrittenToRedis += msg.sizeInBytes();
		totalEventsWrittenToRedis++;
	}

	@Override
	public void onCassandraWriteError(){}

	@Override
	public void report() {
		double timeDiffInSec = (System.currentTimeMillis() - startTime) / 1000.0;
		log.info("Mean read bandwith {} kB/s, {} event/s", (totalBytesRead / timeDiffInSec) / 1000, numOfEventsRead / timeDiffInSec);
		log.info("Mean Cassandra write bandwith {} kB/s, {} event/s", (totalBytesWrittenToCassandra / timeDiffInSec) / 1000, totalEventsWrittenToCassandra / timeDiffInSec);
		log.info("Mean Redis write bandwith {} kB/s, {} event/s", (totalBytesWrittenToRedis / timeDiffInSec) / 1000, totalEventsWrittenToRedis / timeDiffInSec);
	}
}
