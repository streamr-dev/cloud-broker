package com.streamr.broker;

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
	public static final int METRICS_INTERVAL_IN_SECS = Integer.parseInt(System.getProperty("metrics.interval", "3"));
	public static final String METRICS_STREAM_ID = System.getProperty("metrics.stream", "");
	public static final String METRICS_API_KEY = System.getProperty("metrics.apikey", "");
	public static final String METRICS_WS_URL = System.getProperty("metrics.wsurl", "ws://localhost:8890/api/v1/ws");
	public static final String METRICS_REST_URL = System.getProperty("metrics.resturl", "http://localhost:8081/streamr-core/api/v1");

	public static String[] getCassandraHosts() {
		return CASSANDRA_HOSTS.split(",");
	}
}
