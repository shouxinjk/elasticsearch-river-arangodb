package org.elasticsearch.river.arangodb.testclient.query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestDocument {

	private String key;
	private String rev;
	private String text;

	@JsonProperty("_key")
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@JsonProperty("_rev")
	public String getRev() {
		return rev;
	}

	public void setRev(String rev) {
		this.rev = rev;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
