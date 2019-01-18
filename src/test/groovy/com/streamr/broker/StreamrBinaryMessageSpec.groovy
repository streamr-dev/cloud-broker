package com.streamr.broker

import spock.lang.Specification

import java.nio.ByteBuffer

class StreamrBinaryMessageSpec extends Specification {

    StreamrBinaryMessageV28 v28 = new StreamrBinaryMessageV28("testId", 0, System.currentTimeMillis(), 100,
            StreamrBinaryMessageV28.CONTENT_TYPE_STRING, "foobar hello world 666".getBytes("UTF-8"))
    StreamrBinaryMessageV29 v29 = new StreamrBinaryMessageV29("testId", 0, System.currentTimeMillis(), 100,
            StreamrBinaryMessageV28.CONTENT_TYPE_STRING, "foobar hello world 666".getBytes("UTF-8"),
            StreamrBinaryMessageV29.SignatureType.SIGNATURE_TYPE_ETH, '0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4',
            '0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b')

    def "data is not altered on encode/decode for version 28"() {

        when:
        byte[] encoded = v28.toBytes()
        StreamrBinaryMessageV28 decoded = (StreamrBinaryMessageV28) StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(encoded))

        then:
        decoded.getStreamId() == v28.getStreamId()
        decoded.getPartition() == v28.getPartition()
        decoded.getTimestamp() == v28.getTimestamp()
        decoded.getTTL() == v28.getTTL()
        decoded.getContentType() == v28.getContentType()
        new String(decoded.getContentBytes(), "UTF-8") == new String(v28.getContentBytes(), "UTF-8")
    }

    def "data is not altered on encode/decode for version 29"() {

        when:
        byte[] encoded = v29.toBytes()
        StreamrBinaryMessageV29 decoded = (StreamrBinaryMessageV29) StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(encoded))

        then:
        decoded.getStreamId() == v29.getStreamId()
        decoded.getPartition() == v29.getPartition()
        decoded.getTimestamp() == v29.getTimestamp()
        decoded.getTTL() == v29.getTTL()
        decoded.getContentType() == v29.getContentType()
        new String(decoded.getContentBytes(), "UTF-8") == new String(v29.getContentBytes(), "UTF-8")
        decoded.getAddress() == v29.getAddress()
        decoded.getSignatureType() == v29.getSignatureType()
        decoded.getSignature() == v29.getSignature()
    }

    def "sizeInBytes reports correct size"() {
        expect:
        v28.toBytes().length == v28.sizeInBytes()
        v29.toBytes().length == v29.sizeInBytes()
    }

}
