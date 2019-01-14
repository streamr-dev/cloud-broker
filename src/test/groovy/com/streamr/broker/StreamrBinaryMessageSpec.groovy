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
    StreamrBinaryMessageV30 v30Eth = new StreamrBinaryMessageV30("testId", 0, System.currentTimeMillis(), 0,
            '0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4', System.currentTimeMillis() - 100, 0, 100, StreamrBinaryMessageV28.CONTENT_TYPE_STRING,
            "foobar hello world 666".getBytes("UTF-8"), StreamrBinaryMessageV29.SignatureType.SIGNATURE_TYPE_ETH,
            '0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b')
    StreamrBinaryMessageV30 v30String = new StreamrBinaryMessageV30("testId", 0, System.currentTimeMillis(), 0,
            'some-publisher-string-id', System.currentTimeMillis() - 100, 0, 100, StreamrBinaryMessageV28.CONTENT_TYPE_STRING,
            "foobar hello world 666".getBytes("UTF-8"), StreamrBinaryMessageV29.SignatureType.SIGNATURE_TYPE_ETH,
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

    def "data is not altered on encode/decode for version 30 (Ethereum address as publisher id)"() {

        when:
        byte[] encoded = v30Eth.toBytes()
        StreamrBinaryMessageV30 decoded = (StreamrBinaryMessageV30) StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(encoded))

        then:
        decoded.getStreamId() == v30Eth.getStreamId()
        decoded.getPartition() == v30Eth.getPartition()
        decoded.getTimestamp() == v30Eth.getTimestamp()
        decoded.getSequenceNumber() == v30Eth.getSequenceNumber()
        decoded.getPublisherId() == v30Eth.getPublisherId()
        decoded.getPrevTimestamp() == v30Eth.getPrevTimestamp()
        decoded.getPrevSequenceNumber() == v30Eth.getPrevSequenceNumber()
        decoded.getTTL() == v30Eth.getTTL()
        decoded.getContentType() == v30Eth.getContentType()
        new String(decoded.getContentBytes(), "UTF-8") == new String(v30Eth.getContentBytes(), "UTF-8")
        decoded.getSignatureType() == v30Eth.getSignatureType()
        decoded.getSignature() == v30Eth.getSignature()
    }

    def "data is not altered on encode/decode for version 30 (String name as publisher id)"() {

        when:
        byte[] encoded = v30String.toBytes()
        StreamrBinaryMessageV30 decoded = (StreamrBinaryMessageV30) StreamrBinaryMessageFactory.fromBytes(ByteBuffer.wrap(encoded))

        then:
        decoded.getStreamId() == v30String.getStreamId()
        decoded.getPartition() == v30String.getPartition()
        decoded.getTimestamp() == v30String.getTimestamp()
        decoded.getSequenceNumber() == v30String.getSequenceNumber()
        decoded.getPublisherId() == v30String.getPublisherId()
        decoded.getPrevTimestamp() == v30String.getPrevTimestamp()
        decoded.getPrevSequenceNumber() == v30String.getPrevSequenceNumber()
        decoded.getTTL() == v30String.getTTL()
        decoded.getContentType() == v30String.getContentType()
        new String(decoded.getContentBytes(), "UTF-8") == new String(v30String.getContentBytes(), "UTF-8")
        decoded.getSignatureType() == v30String.getSignatureType()
        decoded.getSignature() == v30String.getSignature()
    }

    def "sizeInBytes reports correct size"() {
        expect:
        v28.toBytes().length == v28.sizeInBytes()
        v29.toBytes().length == v29.sizeInBytes()
        v30Eth.toBytes().length == v30Eth.sizeInBytes()
        v30String.toBytes().length == v30String.sizeInBytes()
    }

}
