package com.streamr.broker

class ExampleData {
	private static final StreamrBinaryMessageV30 msg1 = new StreamrBinaryMessageV30(
			"stream-1",
			1,
			1L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			0L,
			0,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"first message".bytes,
			StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_1 = new StreamrBinaryMessageWithKafkaMetadata(
		msg1,
		0,
		1,
		0
	)
	private static final StreamrBinaryMessageV30 msg2 = new StreamrBinaryMessageV30(
			"stream-2",
			0,
			2L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			1L,
			0,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"second message".bytes,
			StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_2 = new StreamrBinaryMessageWithKafkaMetadata(
		msg2,
		0,
		1,
		0
	)
	private static final StreamrBinaryMessageV30 msg3 = new StreamrBinaryMessageV30(
			"stream-3",
			0,
			2L,
			0,
			"0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4",
			1L,
			0,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"third message".bytes,
			StreamrBinaryMessage.SignatureType.SIGNATURE_TYPE_ETH,
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
	)
	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_3 = new StreamrBinaryMessageWithKafkaMetadata(
			msg3,
			0,
			1,
			0
	)
}
