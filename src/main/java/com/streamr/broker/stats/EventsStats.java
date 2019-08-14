package com.streamr.broker.stats;

import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;

public abstract class EventsStats implements Stats {
    protected static final Logger log = LogManager.getLogger();

    private String name;
    private int intervalInSec = -1;

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

    protected int reservedMessageSemaphores = 0;
    protected int reservedCassandraSemaphores = 0;

    public EventsStats(String name, int intervalInSec) {
        this.name = name;
        this.intervalInSec = intervalInSec;
    }

    @Override
    public void start() {
        log.info("{} started. Interval is {} sec(s).", name, intervalInSec);
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
            logReport(null);
        } else {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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

            ReportResult reportResult = new ReportResult(lastDate, kbBackPressure, eventBackPressure, kbReadPerSec,
                    eventReadPerSec, kbWritePerSec, eventWritePerSec, writeErrors);

            logReport(reportResult);
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

    @Override
    public int getIntervalInSec() {
        return intervalInSec;
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

    public abstract void logReport(ReportResult reportResult);
}
