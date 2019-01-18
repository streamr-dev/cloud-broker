package com.streamr.broker;

import com.streamr.client.protocol.message_layer.StreamMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class QueueConsumer implements Runnable {
	private static final Logger log = LogManager.getLogger();

	private final BlockingQueue<StreamMessage> queue;
	private final List<Reporter> reporters;

	QueueConsumer(BlockingQueue<StreamMessage> queue, Reporter... reporterArgs) {
		this.queue = queue;
		this.reporters = Arrays.asList(reporterArgs);
	}

	@Override
	public void run() {
		try {
			while (true) {
				StreamMessage msg = queue.take();
				for (Reporter reporter : reporters) {
					reporter.report(msg);
				}
			}
		} catch (InterruptedException e) {
			log.info("Interrupted.");
		} catch  (Throwable e) {
			log.catching(e);
		} finally {
			log.info("Aborting...");
			for (Reporter reporter : reporters) {
				reporter.close();
			}
		}
	}
}
