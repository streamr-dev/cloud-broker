package com.streamr.broker;

import com.streamr.broker.cassandra.CassandraBatchReporter;
import com.streamr.broker.kafka.KafkaListener;
import com.streamr.broker.redis.RedisReporter;
import com.streamr.broker.stats.LoggedStats;
import com.streamr.broker.stats.MetricsStats;
import com.streamr.broker.stats.Stats;

import java.util.ArrayList;

public class Main {

	private static LoggedStats loggedStats;

	public static void main(String[] args) {
		BrokerProcess brokerProcess = new BrokerProcess(Config.QUEUE_SIZE);
		ArrayList<Stats> stats = new ArrayList<>();
		if (!Config.METRICS_STREAM_ID.equals("")) {
			stats.add(new MetricsStats(Config.METRICS_INTERVAL_IN_SECS, Config.METRICS_STREAM_ID,
					Config.METRICS_API_KEY, Config.METRICS_WS_URL,Config.METRICS_REST_URL));
		}
		loggedStats = new LoggedStats(Config.STATS_INTERVAL_IN_SECS);
		stats.add(loggedStats);
		Stats[] statsArray = new Stats[stats.size()];
		for (int i = 0; i < stats.size(); i++) {
			statsArray[i] = stats.get(i);
		}
		brokerProcess.setStats(statsArray);
		brokerProcess.setUpProducer((queueProducer ->
			new KafkaListener(Config.KAFKA_HOST, Config.KAFKA_GROUP, Config.KAFKA_TOPIC, queueProducer)));
		brokerProcess.setUpConsumer(
			new RedisReporter(Config.REDIS_HOST, Config.REDIS_PORT, Config.REDIS_PASSWORD),
			new CassandraBatchReporter(Config.getCassandraHosts(), Config.CASSANDRA_KEYSPACE, Config.CASSANDRA_USERNAME, Config.CASSANDRA_PASSWORD)
		);
		brokerProcess.startAll();
	}

	public static LoggedStats getStats() {
		return loggedStats;
	}
}
