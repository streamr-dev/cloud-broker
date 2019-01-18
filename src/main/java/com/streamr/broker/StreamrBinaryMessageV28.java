package com.streamr.broker;

import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.protocol.message_layer.StreamMessageV28;

import java.io.IOException;

public class StreamrBinaryMessageV28 extends StreamrBinaryMessage {
    public static final byte VERSION = 28; //0x1C

    public StreamrBinaryMessageV28(String streamId, int partition, long timestamp, int ttl, byte contentType, byte[] content) {
        super(VERSION, streamId, partition, timestamp, ttl, contentType, content);
    }

    @Override
    public StreamMessageV28 toStreamrMessage(Long offset, Long previousOffset) throws IOException {
        return new StreamMessageV28(streamId, partition, timestamp, ttl, offset, previousOffset, StreamMessage.ContentType.fromId(contentType), toString());
    }
}
