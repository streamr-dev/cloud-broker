package com.streamr.broker;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class StreamrBinaryMessage {
	private static final byte VERSION = 28; //0x1C

	public static final byte CONTENT_TYPE_STRING = 11; //0x0B
	public static final byte CONTENT_TYPE_JSON = 27; //0x1B

	private static final Charset utf8 = Charset.forName("UTF-8");

	private final String streamId;
	private final int partition;
	private final long timestamp;
	private final byte contentType;
	private final byte[] streamIdAsBytes;
	private final byte[] content;
	private final int ttl;

	public StreamrBinaryMessage(ByteBuffer bb) {
		byte version = bb.get();

		// If the message starts with a version byte, parse timestamp and contentType headers
		if (version==28) {
			timestamp = bb.getLong();
			ttl = bb.getInt();
			int streamIdLength = bb.get() & 0xFF; // unsigned byte
			streamIdAsBytes = new byte[streamIdLength];
			bb.get(streamIdAsBytes);
			streamId = new String(streamIdAsBytes, utf8);
			partition = bb.get() & 0xff; // unsigned byte
			contentType = bb.get();
			int contentLength = bb.getInt();
			content = new byte[contentLength];
			bb.get(content);
		}
		else {
			throw new IllegalArgumentException("Unknown version byte: "+version);
		}
	}

	public StreamrBinaryMessage(String streamId, int partition, long timestamp, int ttl, byte contentType, byte[] content) {
		this.streamId = streamId;
		this.partition = partition;
		this.streamIdAsBytes = this.streamId.getBytes(utf8);
		this.timestamp = timestamp;
		this.ttl = ttl;
		this.contentType = contentType;
		this.content = content;
	}

	/**
	 * 	version 1 byte
	 * 	timestamp 8 bytes
	 * 	stream id length 1 byte (interpret as unsigned)
	 * 	stream id, N bytes
	 * 	content type 1 byte
	 * 	content length 4 bytes
	 * 	payload, N bytes
	 * 	ttl, 4 bytes
	 */
	public byte[] toBytes() {
		ByteBuffer bb;
		bb = ByteBuffer.allocate(20+streamIdAsBytes.length+content.length); // 20 == version + timestamp + ttl + stream id length + partition + content type + content length + content
		bb.put(VERSION); // 1 byte
		bb.putLong(timestamp); // 8 bytes
		bb.putInt(ttl); // 4 bytes
		if (streamIdAsBytes.length > 255) {
			throw new IllegalArgumentException("Stream id too long: "+streamId+", length "+streamIdAsBytes.length);
		}
		bb.put((byte) streamIdAsBytes.length); // 1 byte
		bb.put(streamIdAsBytes);
		if (partition > 255) {
			throw new IllegalArgumentException("Partition out of range: "+partition);
		}
		bb.put((byte) partition); // 1 byte
		bb.put(contentType); // 1 byte
		bb.putInt(content.length); // 4 bytes
		bb.put(content); // contentLength bytes
		return bb.array();
	}

	public String getStreamId() {
		return streamId;
	}

	public byte[] getContentBytes() {
		return content;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getPartition() { return partition; }

	public byte getContentType() {
		return contentType;
	}

	public int getTTL() {
		return ttl;
	}

	@Override
	public String toString() {
		if (contentType==CONTENT_TYPE_STRING || contentType==CONTENT_TYPE_JSON)
			try {
				return new String(getContentBytes(),"UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Platform does not support UTF-8!");
			}
		else return super.toString();
	}

}
