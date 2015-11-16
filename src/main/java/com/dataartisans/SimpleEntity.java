package com.dataartisans;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.Stringable;


public class SimpleEntity {
	@Stringable
	private String id;
	@Nullable
	private Long value;

	public SimpleEntity() {
	}

	public SimpleEntity(String id, Long value) {
		this.id = id;
		this.value = value;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SimpleEntity{" +
				"id='" + id + '\'' +
				", value=" + value +
				'}';
	}
}