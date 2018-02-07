package com.streamr.broker;

import com.lambdaworks.redis.pubsub.RedisPubSubListener;

import java.util.List;

public class TestRedisListener implements RedisPubSubListener<byte[], byte[]> {

	private final List<byte[]> messages;

	public TestRedisListener(List<byte[]> messages) {
		this.messages = messages;
	}

	@Override
	public void message(byte[] channel, byte[] message) {
		messages.add(message);
	}

	@Override
	public void message(byte[] pattern, byte[] channel, byte[] message) {

	}

	@Override
	public void subscribed(byte[] channel, long count) {

	}

	@Override
	public void psubscribed(byte[] pattern, long count) {

	}

	@Override
	public void unsubscribed(byte[] channel, long count) {

	}

	@Override
	public void punsubscribed(byte[] pattern, long count) {

	}

	public List<byte[]> getMessages() {
		return messages;
	}
}
