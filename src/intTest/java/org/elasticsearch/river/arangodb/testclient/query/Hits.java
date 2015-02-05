package org.elasticsearch.river.arangodb.testclient.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Hits {

	private long total;
	private double maxScore;
	private List<Hit> hits;

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	@JsonProperty("max_score")
	public double getMaxScore() {
		return maxScore;
	}

	public void setMaxScore(double maxScore) {
		this.maxScore = maxScore;
	}

	public List<Hit> getHits() {
		return hits;
	}

	public void setHits(List<Hit> hits) {
		this.hits = hits;
	}
}
