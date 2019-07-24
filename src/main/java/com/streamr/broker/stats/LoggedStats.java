package com.streamr.broker.stats;

import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.EncryptionOptions;
import com.streamr.client.options.SigningOptions;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.StreamrClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LoggedStats implements Stats {
	private static final Logger log = LogManager.getLogger();
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private int intervalInSec = -1;
	private int metricsIntervalInSec = -1;

	private long lastTimestamp = 0;
	private AtomicLong totalBytesRead = new AtomicLong(0);
	private AtomicLong totalBytesWritten = new AtomicLong(0);
	private AtomicLong totalEventsRead = new AtomicLong(0);
	private AtomicLong totalEventsWritten = new AtomicLong(0);
	private AtomicLong totalEventsWrittenRedis = new AtomicLong(0);
	private AtomicLong totalWriteErrors = new AtomicLong(0);

	private long lastBytesRead = 0;
	private long lastBytesWritten = 0;
	private long lastEventsRead = 0;
	private long lastEventsWritten = 0;
	private long lastWriteErrors = 0;

	private int reservedMessageSemaphores = 0;
	private int reservedCassandraSemaphores = 0;

	private static final String TEMPLATE = "\n" +
			"\tLast timestamp {}\n" +
			"\tBackpressure {} kB / {} events\n" +
			"\tRead throughput {} kB/s or {} event/s\n" +
			"\tWrite throughput {} kB/s or {} event/s\n" +
			"\tWrite errors {}\n" +
			"\tReserved message semaphores: {}\n" +
			"\tReserved cassandra semaphores: {}";

	private StreamrClient client = null;
	private String metricsStreamId;

	public LoggedStats() {

	}

	public LoggedStats(int metricsIntervalInSec,
					   String metricsStreamId, String metricsApiKey,
					   String wsUrl, String restUrl) {
		this.metricsStreamId = metricsStreamId;
		StreamrClientOptions options = new StreamrClientOptions(new ApiKeyAuthenticationMethod(metricsApiKey),
				SigningOptions.getDefault(), EncryptionOptions.getDefault(), wsUrl, restUrl);
		client = new StreamrClient(options);
		this.metricsIntervalInSec = metricsIntervalInSec;
		log.info("Metrics reporting interval to stream {} is {} sec(s).", metricsStreamId, metricsIntervalInSec);
	}

	@Override
	public void start(int intervalInSec) {
		this.intervalInSec = intervalInSec;
		log.info("Statistics logger started. Logging interval is {} sec(s).", intervalInSec);
	}

	@Override
	public void stop() {}

	@Override
	public void onReadFromKafka(StreamMessage msg) {
		totalEventsRead.incrementAndGet();
		totalBytesRead.addAndGet(msg.sizeInBytes());
		lastTimestamp = msg.getTimestamp();
	}

	@Override
	public void onWrittenToCassandra(StreamMessage msg) {
		totalEventsWritten.incrementAndGet();
		totalBytesWritten.addAndGet(msg.sizeInBytes());
	}

	@Override
	public void onWrittenToRedis(StreamMessage msg) {
		totalEventsWrittenRedis.incrementAndGet();
	}

	@Override
	public void onCassandraWriteError() {
		totalWriteErrors.incrementAndGet();
	}

	@Override
	public void report() {
		if (lastBytesRead == totalBytesRead.get()) {
			log.info("No new data.");
		} else {
			String lastDate = dateFormat.format(lastTimestamp);
			double kbBackPressure = (totalBytesRead.get() - totalBytesWritten.get()) / 1000.0;
			double kbReadSinceLastReport = (totalBytesRead.get() - lastBytesRead) / 1000.0;
			double kbWrittenSinceLastReport = (totalBytesWritten.get() - lastBytesWritten) / 1000.0;
			long eventBackPressure = totalEventsRead.get() - totalEventsWritten.get();
			long eventsReadSinceLastReport = totalEventsRead.get() - lastEventsRead;
			long eventsWrittenSinceLastReport = totalEventsWritten.get() - lastEventsWritten;
			double kbWritePerSec = kbWrittenSinceLastReport / intervalInSec;
			long eventWritePerSec = eventsWrittenSinceLastReport / intervalInSec;
			double kbReadPerSec = kbReadSinceLastReport / intervalInSec;
			long eventReadPerSec = eventsReadSinceLastReport / intervalInSec;
			long writeErrors = totalWriteErrors.get() - lastWriteErrors;

			lastBytesRead = totalBytesRead.get();
			lastBytesWritten = totalBytesWritten.get();
			lastEventsRead = totalEventsRead.get();
			lastEventsWritten = totalEventsWritten.get();
			lastWriteErrors = totalWriteErrors.get();

			log.info(TEMPLATE, lastDate, kbBackPressure, eventBackPressure, kbReadPerSec, eventReadPerSec,
					kbWritePerSec, eventWritePerSec, writeErrors, reservedMessageSemaphores, reservedCassandraSemaphores);
		}
	}

	@Override
	public void reportToStream() {
		double kbReadSinceLastReport = (totalBytesRead.get() - lastBytesRead) / 1000.0;
		long eventsReadSinceLastReport = totalEventsRead.get() - lastEventsRead;
		double kbReadPerSec = kbReadSinceLastReport / metricsIntervalInSec;
		long eventReadPerSec = eventsReadSinceLastReport / intervalInSec;
		Map<String, Object> payload = new HashMap<>();
		payload.put("kbReadPerSec", kbReadPerSec);
		payload.put("eventReadPerSec", eventReadPerSec);
		try {
			client.publish(client.getStream(this.metricsStreamId), payload);
		} catch (IOException e) {
			log.error(e);
		}
	}

	@Override
	public void setReservedMessageSemaphores(int reservedMessageSemaphores) {
		this.reservedMessageSemaphores = reservedMessageSemaphores;
	}

	@Override
	public void setReservedCassandraSemaphores(int reservedCassandraSemaphores) {
		this.reservedCassandraSemaphores = reservedCassandraSemaphores;
	}

	public long getLastTimestamp() {
		return lastTimestamp;
	}

	public long getTotalBytesRead() {
		return totalBytesRead.get();
	}

	public long getTotalBytesWritten() {
		return totalBytesWritten.get();
	}

	public long getTotalEventsRead() {
		return totalEventsRead.get();
	}

	public long getTotalEventsWritten() {
		return totalEventsWritten.get();
	}

	public long getTotalEventsWrittenRedis() {
		return totalEventsWrittenRedis.get();
	}

	public long getTotalWriteErrors() {
		return totalWriteErrors.get();
	}

	public long getLastBytesRead() {
		return lastBytesRead;
	}

	public long getLastBytesWritten() {
		return lastBytesWritten;
	}

	public long getLastEventsWritten() {
		return lastEventsWritten;
	}

	public long getLastWriteErrors() {
		return lastWriteErrors;
	}
}
