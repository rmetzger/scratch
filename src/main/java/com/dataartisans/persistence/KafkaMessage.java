package com.dataartisans.persistence;

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
}
