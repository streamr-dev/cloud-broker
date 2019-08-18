package com.streamr.broker.stats;

public class LoggedStats extends EventsStats {
	private static final String TEMPLATE = "\n" +
			"\tLast timestamp {}\n" +
			"\tBackpressure {} kB / {} events\n" +
			"\tRead throughput {} kB/s or {} event/s\n" +
			"\tWrite throughput {} kB/s or {} event/s\n" +
			"\tWrite errors {}\n" +
			"\tReserved message semaphores: {}\n" +
			"\tReserved cassandra semaphores: {}";

	public LoggedStats(int intervalInSec) {
		super("Statistics logger", intervalInSec);
	}

	@Override
	public void logReport(ReportResult reportResult) {
		if (reportResult == null) {
			log.info("No new data.");
		} else {
			log.info(TEMPLATE,
					reportResult.getLastDate(),
					reportResult.getKbBackPressure(),
					reportResult.getEventBackPressure(),
					reportResult.getKbReadPerSec(),
					reportResult.getEventReadPerSec(),
					reportResult.getKbWritePerSec(),
					reportResult.getEventWritePerSec(),
					reportResult.getWriteErrors(),
					reservedMessageSemaphores,
					reservedCassandraSemaphores);
		}
	}
}
