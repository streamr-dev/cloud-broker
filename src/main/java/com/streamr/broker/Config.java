package com.streamr.broker;

import java.util.HashSet;
import java.util.Set;

public class Config {
	public static final String KAFKA_HOST = System.getProperty("kafka.server", "127.0.0.1:9092");
	public static final String KAFKA_GROUP = System.getProperty("kafka.group", "data-dev");
	public static final String KAFKA_TOPIC = System.getProperty("kafka.topic", "data-dev");
	public static final String REDIS_HOST = System.getProperty("redis.host", "127.0.0.1");
	public static final int REDIS_PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));
	public static final String REDIS_PASSWORD = System.getProperty("redis.password", "");
	public static final String CASSANDRA_HOSTS = System.getProperty("cassandra.hosts", "127.0.0.1");
	public static final String CASSANDRA_KEYSPACE = System.getProperty("cassandra.keyspace", "streamr_dev");
	public static final String CASSANDRA_USERNAME = System.getProperty("cassandra.username", "");
	public static final String CASSANDRA_PASSWORD = System.getProperty("cassandra.password", "");
	public static final int QUEUE_SIZE = Integer.parseInt(System.getProperty("queuesize", "200"));
	public static final int STATS_INTERVAL_IN_SECS = Integer.parseInt(System.getProperty("statsinterval", "30"));

	public static final String NODES = System.getProperty("nodes", "");
	public static final String STREAM_FILTER = System.getProperty("stream.filter", "");

	public static String[] getCassandraHosts() {
		return CASSANDRA_HOSTS.split(",");
	}

	public static String[] getNodes() {
		String[] nodes = NODES.split(",");
		if (nodes.length == 0) {
			throw new IllegalArgumentException("No nodes given! Use -Dnodes=ws://ip:port/path,ws://ip2:port/path");
		}
		return nodes;
	}

	public static Set<String> getStreamFilter() {
		Set<String> result = new HashSet<>();
		for (String streamId : STREAM_FILTER.split(",")) {
			if (!streamId.isEmpty()) {
				result.add(streamId);
			}
		}
		return result;
	}
}
