package org.elasticsearch.river.arangodb.testclient.query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Hit {

	private String index;
	private String type;
	private String id;
	private double score;
	private TestDocument source;

	@JsonProperty("_index")
	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	@JsonProperty("_type")
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@JsonProperty("_id")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@JsonProperty("_score")
	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@JsonProperty("_source")
	public TestDocument getSource() {
		return source;
	}

	public void setSource(TestDocument source) {
		this.source = source;
	}
}
