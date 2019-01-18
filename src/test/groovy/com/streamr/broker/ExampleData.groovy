package com.streamr.broker

import com.streamr.client.protocol.message_layer.StreamMessage.ContentType
import com.streamr.client.protocol.message_layer.StreamMessage.SignatureType
import com.streamr.client.protocol.message_layer.StreamMessageV30

class ExampleData {
	static final StreamMessageV30 MESSAGE_1 = new StreamMessageV30(
			"stream-1",
			1,
			1L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			0L,
			0,
			ContentType.CONTENT_TYPE_JSON,
			'{"first": "message"}',
			SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
	static final StreamMessageV30 MESSAGE_2 = new StreamMessageV30(
			"stream-2",
			0,
			2L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			0L,
			0,
			ContentType.CONTENT_TYPE_JSON,
			'{"second": "message"}',
			SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
	static final StreamMessageV30 MESSAGE_3 = new StreamMessageV30(
			"stream-3",
			0,
			2L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			0L,
			0,
			ContentType.CONTENT_TYPE_JSON,
			'{"third": "message"}',
			SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
}
