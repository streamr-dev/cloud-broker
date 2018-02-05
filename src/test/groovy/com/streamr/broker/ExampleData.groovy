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
}
