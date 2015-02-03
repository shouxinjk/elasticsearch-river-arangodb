package org.elasticsearch.river.arangodb.client;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Index {

	private String name;
	private String type;
	private int bulkSize;
	private String bulkTimeout;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@JsonProperty("bulk_size")
	public int getBulkSize() {
		return bulkSize;
	}

	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}

	@JsonProperty("bulk_timeout")
	public String getBulkTimeout() {
		return bulkTimeout;
	}

	public void setBulkTimeout(String bulkTimeout) {
		this.bulkTimeout = bulkTimeout;
	}
}
