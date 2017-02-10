package com.streamr.broker;

class PerformanceTestConfiguration {
	static final int QUEUE_SIZE = 2000;
	static final int NUM_OF_MESSAGES = 1000000;
	static final int[] SMALL_PAYLOAD_RANGE = new int[]{100, 400}; // ~ millistream price and trades size range
	static final int[] LARGE_PAYLOAD_RANGE = new int[]{2500, 72000}; // millistream news size range
	static final String[] STREAM_IDS = new String[]{"stream-1", "stream-2", "stream-3", "stream-4", "stream-5",
		"stream-6", "stream-7", "stream-8", "stream-9", "stream-10", "stream-11", "stream-12"
	};
}
