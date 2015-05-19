package com.dataartisans.persistence;

import java.util.Arrays;

/**
 * Created by robert on 5/4/15.
 */
public class KafkaMessage {
	public long offset;
	public int partition;
	public byte[] data;

	public KafkaMessage() {}

	public KafkaMessage(long offset, int partition, byte[] data) {
		this.offset = offset;
		this.partition = partition;
		this.data = data;
	}

	@Override
	public String toString() {
		return "KafkaMessage{" +
				"offset=" + offset +
				", partition=" + partition +
				", data.length=" + data.length +
				'}';
	}
}
