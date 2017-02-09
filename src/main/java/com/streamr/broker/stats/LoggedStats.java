package com.streamr.broker.stats;

import com.streamr.broker.StreamrBinaryMessageWithKafkaMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

// TODO: synchronization
public class LoggedStats implements Stats, Runnable {
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final Logger log = LogManager.getLogger();

	private final int intervalInSec;
	private long lastTimestamp = 0;
	private long bytesRead = 0;
	private long bytesWritten = 0;
	private int eventsRead = 0;
	private int eventsWritten = 0;
	private long lastBytesWritten = 0;
	private int lastEventsWritten = 0;

	public LoggedStats(int intervalInSec) {
		this.intervalInSec = intervalInSec;
		log.info("Statistics reported every {} seconds", intervalInSec);
	}

	@Override
	public void onReadFromKafka(StreamrBinaryMessageWithKafkaMetadata msg) {
		eventsRead++;
		bytesRead += msg.sizeInBytes();
		lastTimestamp = msg.getTimestamp();
	}

	@Override
	public void onWrittenToCassandra(StreamrBinaryMessageWithKafkaMetadata msg) {
		eventsWritten++;
		bytesWritten += msg.sizeInBytes();
	}

	@Override
	public void run() {
		log.info("Last timestamp {}. Backpressure {} kB (={}-{})  [{} events (={}-{})]",
			dateFormat.format(lastTimestamp),
			(bytesRead - bytesWritten) / 1000.0, (bytesRead - lastBytesWritten) / 1000.0, (bytesWritten - lastBytesWritten) / 1000.0,
			eventsRead - eventsWritten, eventsRead - lastEventsWritten,eventsWritten - lastEventsWritten);
		log.info("Write throughput {} kB/s ({} event/s)", ((bytesWritten - lastBytesWritten) / 1000.0) / intervalInSec, (eventsWritten - lastEventsWritten) / intervalInSec);
		lastBytesWritten = bytesWritten;
		lastEventsWritten = eventsWritten;
	}

	public int getIntervalInSec() {
		return intervalInSec;
	}
}
