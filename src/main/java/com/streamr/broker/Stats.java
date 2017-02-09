package com.streamr.broker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

// TODO: synchronization
public class Stats {
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final Logger log = LogManager.getLogger();

	public long bytesRead = 0;
	public long bytesWritten = 0;
	public int eventsRead = 0;
	public int eventsWritten = 0;
	public long lastTimestamp = 0;

	private final int statsIntervalSecs;
	private long lastBytesWritten = 0;
	private int lastEventsWritten = 0;

	Stats(int statsIntervalSecs) {
		this.statsIntervalSecs = statsIntervalSecs;
	}

	void report() {
		log.info("Last timestamp {}. Backpressure {} kB (={}-{})  [{} events (={}-{})]",
			dateFormat.format(lastTimestamp),
			(bytesRead - bytesWritten) / 1000.0, (bytesRead - lastBytesWritten) / 1000.0, (bytesWritten - lastBytesWritten) / 1000.0,
			eventsRead - eventsWritten, eventsRead - lastEventsWritten,eventsWritten - lastEventsWritten);
		log.info("Write throughput {} kB/s ({} event/s)", ((bytesWritten - lastBytesWritten) / 1000.0) / statsIntervalSecs, (eventsWritten - lastEventsWritten) / statsIntervalSecs);
		lastBytesWritten = bytesWritten;
		lastEventsWritten = eventsWritten;
	}
}
