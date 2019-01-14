package com.streamr.broker;

import java.util.Date;
import java.util.Map;

public class StreamrMessage {
    public final Date timestamp;
    public final Map payload;
    private final int partition;
    private final String streamId;
    private final StreamrBinaryMessage.SignatureType signatureType;
    private final String address;
    private final String signature;

    public StreamrMessage(String streamId, int partition, Date timestamp, Map content) {
        this.timestamp = timestamp;
        this.payload = content;
        this.streamId = streamId;
        this.partition = partition;
        this.signatureType = StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_NONE;
        this.address = null;
        this.signature = null;
    }

    public StreamrMessage(String streamId, int partition, Date timestamp, Map content,
                          StreamrBinaryMessage.SignatureType signatureType, String address, String signature) {
        this.timestamp = timestamp;
        this.payload = content;
        this.streamId = streamId;
        this.partition = partition;
        this.signatureType = signatureType;
        this.address = address;
        this.signature = signature;
    }

    public int getPartition() {
        return partition;
    }

    public String getStreamId() {
        return streamId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "StreamrMessage{" +
                "partition=" + partition +
                ", streamId='" + streamId + '\'' +
                ", timestamp=" + timestamp +
                ", payload=" + payload +
                '}';
    }
}
