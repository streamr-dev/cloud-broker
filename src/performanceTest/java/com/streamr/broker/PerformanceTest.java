package com.streamr.broker;

import java.util.concurrent.ExecutionException;

public class PerformanceTest {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new KafkaSpammer("127.0.0.1:9092", "data-dev").spam();
		Main.main(args);
	}
}
