package org.elasticsearch.river.arangodb.testclient.query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchResponse {

	private int status;
	private String error;
	private long took;
	private boolean timedOut;
	private Shards shards;
	private Hits hits;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public long getTook() {
		return took;
	}

	public void setTook(long took) {
		this.took = took;
	}

	@JsonProperty("timed_out")
	public boolean getTimedOut() {
		return timedOut;
	}

	public void setTimedOut(boolean timedOut) {
		this.timedOut = timedOut;
	}

	@JsonProperty("_shards")
	public Shards getShards() {
		return shards;
	}

	public void setShards(Shards shards) {
		this.shards = shards;
	}

	public Hits getHits() {
		return hits;
	}

	public void setHits(Hits hits) {
		this.hits = hits;
	}
}
