package com.streamr.broker

class ExampleData {
	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_1 = new StreamrBinaryMessageWithKafkaMetadata(
		"stream-1",
		1,
		1L,
		1,
		StreamrBinaryMessage.CONTENT_TYPE_STRING,
		"first message".bytes,
		0,
		1,
		0
	)

	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_2 = new StreamrBinaryMessageWithKafkaMetadata(
		"stream-2",
		0,
		2L,
		1,
		StreamrBinaryMessage.CONTENT_TYPE_STRING,
		"second message".bytes,
		0,
		1,
		0
	)

	static final StreamrBinaryMessageWithKafkaMetadata MESSAGE_3 = new StreamrBinaryMessageWithKafkaMetadata(
			(byte) 29,
			"stream-3",
			0,
			2L,
			1,
			StreamrBinaryMessage.CONTENT_TYPE_STRING,
			"second message".bytes,
			StreamrBinaryMessage.SIGNATURE_TYPE_ETH,
			'0xF915eD664e43C50eB7b9Ca7CfEB992703eDe55c4',
			'0xcb1fa20f2f8e75f27d3f171d236c071f0de39e4b497c51b390306fc6e7e112bb415ecea1bd093320dd91fd91113748286711122548c52a15179822a014dc14931b',
			0,
			1,
			0
	)
}
