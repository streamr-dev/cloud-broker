package com.streamr.broker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class QueueConsumer implements Runnable {
	private static final Logger log = LogManager.getLogger();

	private final BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue;
	private final List<Reporter> reporters;

	QueueConsumer(BlockingQueue<StreamrBinaryMessageWithKafkaMetadata> queue, Reporter... reporterArgs) {
		this.queue = queue;
		this.reporters = Arrays.asList(reporterArgs);
	}

	@Override
	public void run() {
		try {
			while (true) {
				StreamrBinaryMessageWithKafkaMetadata msg = queue.take();
				for (Reporter reporter : reporters) {
					reporter.report(msg);
				}
			}
		} catch (InterruptedException e) {
			log.catching(e);
		} finally {
			log.info("Aborting...");
			for (Reporter reporter : reporters) {
				reporter.close();
			}
		}
	}
}
