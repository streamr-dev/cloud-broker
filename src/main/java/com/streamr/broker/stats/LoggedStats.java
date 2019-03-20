package com.streamr.broker.stats;

import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

// TODO: synchronization
public class LoggedStats implements Stats {
	private static final Logger log = LogManager.getLogger();
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private int intervalInSec = -1;

	private long lastTimestamp = 0;
	private long totalBytesRead = 0;
	private long totalBytesWritten = 0;
	private int totalEventsRead = 0;
	private int totalEventsWritten = 0;
	private long totalWriteErrors = 0;

	private long lastBytesRead = 0;
	private long lastBytesWritten = 0;
	private int lastEventsRead = 0;
	private int lastEventsWritten = 0;
	private long lastWriteErrors = 0;

	@Override
	public void start(int intervalInSec) {
		this.intervalInSec = intervalInSec;
		log.info("Statistics logger started. Logging interval is {} sec(s).", intervalInSec);
	}

	@Override
	public void stop() {}

	@Override
	public void onReadFromKafka(StreamMessage msg) {
		totalEventsRead++;
		totalBytesRead += msg.sizeInBytes();
		lastTimestamp = msg.getTimestamp();
	}

	@Override
	public void onWrittenToCassandra(StreamMessage msg) {
		totalEventsWritten++;
		totalBytesWritten += msg.sizeInBytes();
	}

	@Override
	public void onWrittenToRedis(StreamMessage msg) {}

	@Override
	public void onCassandraWriteError() {
		totalWriteErrors++;
	}

	@Override
	public void report() {
		if (lastBytesRead == totalBytesRead) {
			log.info("No new data.");
		} else {
			String lastDate = dateFormat.format(lastTimestamp);
			double kbPackPressure = (totalBytesRead - lastBytesRead) / 1000.0;
			double kbReadSinceLastReport = (totalBytesRead - lastBytesWritten) / 1000.0;
			double kbWrittenSinceLastReport = (totalBytesWritten - lastBytesWritten) / 1000.0;
			long eventBackPressure = totalEventsRead - totalEventsWritten;
			int eventsReadSinceLastReport = totalEventsRead - lastEventsRead;
			int eventsWrittenSinceLastReport = totalEventsWritten - lastEventsWritten;
			double kbWritePerSec = kbWrittenSinceLastReport / intervalInSec;
			int eventWritePerSec = eventsWrittenSinceLastReport / intervalInSec;
			double kbReadPerSec = kbReadSinceLastReport / intervalInSec;
			int eventReadPerSec = eventsReadSinceLastReport / intervalInSec;
			long writeErrors = totalWriteErrors - lastWriteErrors;

			lastBytesRead = totalBytesRead;
			lastBytesWritten = totalBytesWritten;
			lastEventsRead = totalEventsRead;
			lastEventsWritten = totalEventsWritten;
			lastWriteErrors = totalWriteErrors;

			String template = "\n" +
				"\tLast timestamp {}\n" +
				"\tBackpressure {} kB / {} events\n" +
				"\tRead throughput {} kB/s or {} event/s\n" +
				"\tWrite throughput {} kB/s or {} event/s\n" +
				"\tWrite errors {}";

			log.info(template, lastDate, kbPackPressure, eventBackPressure, kbReadPerSec, eventReadPerSec,
					kbWritePerSec, eventWritePerSec, writeErrors);
		}
	}
}
