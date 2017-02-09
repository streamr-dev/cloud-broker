package com.streamr.broker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

class Stats {
	private static final DateFormat dateFormat = SimpleDateFormat.getDateTimeInstance();
	private static final Logger log = LogManager.getLogger();

	long bytesRead = 0;
	long bytesWritten = 0;
	int eventsRead = 0;
	int eventsWritten = 0;
	long lastTimestamp = 0;

	void reportAndReset() {
		if (eventsRead > 0) {
			log.info("Last timestamp {}. Read {} events, {} kB", dateFormat.format(lastTimestamp), eventsRead, bytesRead / 1000.0);
			bytesRead = 0;
			bytesWritten = 0;
			eventsRead = 0;
			eventsWritten = 0;
		}
	}
}
