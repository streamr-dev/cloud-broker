package com.streamr.broker;

import com.streamr.broker.StreamrBinaryMessageV29.SignatureType;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.util.Date;

public class StreamrBinaryMessageV30 extends StreamrBinaryMessage {
    public static final byte VERSION = 30; //0x1E

    private final int sequenceNumber;
    private final long prevTimestamp;
    private final int prevSequenceNumber;
    private final SignatureType signatureType;
    private final byte[] publisherIdBytes;
    private final byte[] signatureBytes;

    public StreamrBinaryMessageV30(String streamId, int partition, long timestamp, int sequenceNumber,
                                   String publisherId, long prevTimestamp, int prevSequenceNumber, int ttl,
                                   byte contentType, byte[] content, SignatureType signatureType, String signature) {
        super(VERSION, streamId, partition, timestamp, ttl, contentType, content);
        this.sequenceNumber = sequenceNumber;
        this.prevTimestamp = prevTimestamp;
        this.prevSequenceNumber = prevSequenceNumber;
        this.signatureType = signatureType;
        this.publisherIdBytes = hexToBytes(publisherId);
        this.signatureBytes = hexToBytes(signature);
    }

    public StreamrBinaryMessageV30(String streamId, int partition, long timestamp, int sequenceNumber,
                                   byte[] publisherIdBytes, long prevTimestamp, int prevSequenceNumber, int ttl,
                                   byte contentType, byte[] content, SignatureType signatureType, byte[] signatureBytes) {
        super(VERSION, streamId, partition, timestamp, ttl, contentType, content);
        this.sequenceNumber = sequenceNumber;
        this.prevTimestamp = prevTimestamp;
        this.prevSequenceNumber = prevSequenceNumber;
        this.signatureType = signatureType;
        this.publisherIdBytes = publisherIdBytes;
        this.signatureBytes = signatureBytes;
    }

    @Override
    protected void toByteBuffer(ByteBuffer bb) {
        bb.put(VERSION); // 1 byte
        if (streamIdAsBytes.length > 255) {
            throw new IllegalArgumentException("Stream id too long: "+streamId+", length "+streamIdAsBytes.length);
        }
        bb.put((byte) streamIdAsBytes.length); // 1 byte
        bb.put(streamIdAsBytes);
        if (partition > 255) {
            throw new IllegalArgumentException("Partition out of range: "+partition);
        }
        bb.put((byte) partition); // 1 byte
        bb.putLong(timestamp); // 8 bytes
        bb.putInt(sequenceNumber); // 4 bytes
        bb.put(publisherIdBytes); // 20 bytes
        bb.putLong(prevTimestamp); // 8 bytes
        bb.putInt(prevSequenceNumber); // 4 bytes
        bb.putInt(ttl); // 4 bytes
        bb.put(contentType); // 1 byte
        bb.putInt(content.length); // 4 bytes
        bb.put(content); // contentLength bytes
        bb.put(signatureType.getId()); // 1 byte
        if (signatureType == SignatureType.SIGNATURE_TYPE_ETH) {
            bb.put(signatureBytes); // 65 bytes
        }
    }

    @Override
    public int sizeInBytes() {
        // add sequenceNumber, publisherId, prevTimestamp, prevSequenceNumber and signatureType
        int size = super.sizeInBytes() + 4 + 20 + 8 + 4 + 1;
        if (signatureType == SignatureType.SIGNATURE_TYPE_NONE) {
            return size;
        } else if (signatureType == SignatureType.SIGNATURE_TYPE_ETH) {
            // add signatureBytes
            return size + 65;
        } else {
            throw new IllegalArgumentException("Unknown signature type: "+signatureType);
        }
    }

    public SignatureType getSignatureType() {
        return signatureType;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPublisherId() {
        return bytesToHex(publisherIdBytes);
    }

    public long getPrevTimestamp() {
        return prevTimestamp;
    }

    public int getPrevSequenceNumber() {
        return prevSequenceNumber;
    }

    public String getSignature() {
        return bytesToHex(signatureBytes);
    }

    private byte[] hexToBytes(String s) {
        if (s == null) {
            return null;
        }
        if (s.startsWith("0x")) {
            return DatatypeConverter.parseHexBinary(s.substring(2));
        }
        return DatatypeConverter.parseHexBinary(s);
    }

    private String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return "0x" + DatatypeConverter.printHexBinary(bytes);
    }

    @Override
    public StreamrMessage toStreamrMessage() {
        return new StreamrMessage(getStreamId(), getPartition(), new Date(getTimestamp()), getContentJSON(), getSignatureType(), getPublisherId(), getSignature());
    }
}
