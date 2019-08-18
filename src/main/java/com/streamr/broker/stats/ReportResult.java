package com.streamr.broker.stats;

public class ReportResult {
    private final String lastDate;
    private final double kbBackPressure;
    private final long eventBackPressure;
    private final double kbReadPerSec;
    private final long eventReadPerSec;
    private final double kbWritePerSec;
    private final long eventWritePerSec;
    private final long writeErrors;

    public ReportResult(String lastDate, double kbBackPressure, long eventBackPressure, double kbReadPerSec, long eventReadPerSec, double kbWritePerSec, long eventWritePerSec, long writeErrors) {
        this.lastDate = lastDate;
        this.kbBackPressure = kbBackPressure;
        this.eventBackPressure = eventBackPressure;
        this.kbReadPerSec = kbReadPerSec;
        this.eventReadPerSec = eventReadPerSec;
        this.kbWritePerSec = kbWritePerSec;
        this.eventWritePerSec = eventWritePerSec;
        this.writeErrors = writeErrors;
    }

    public String getLastDate() {
        return lastDate;
    }

    public double getKbBackPressure() {
        return kbBackPressure;
    }

    public long getEventBackPressure() {
        return eventBackPressure;
    }

    public double getKbReadPerSec() {
        return kbReadPerSec;
    }

    public long getEventReadPerSec() {
        return eventReadPerSec;
    }

    public double getKbWritePerSec() {
        return kbWritePerSec;
    }

    public long getEventWritePerSec() {
        return eventWritePerSec;
    }

    public long getWriteErrors() {
        return writeErrors;
    }
}
