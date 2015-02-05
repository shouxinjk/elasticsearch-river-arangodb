package org.elasticsearch.river.arangodb.testclient.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Arangodb {

	private String host;
	private int port;
	private String db;
	private String collection;
	private Credentials credentials;
	private String readerMinSleep;
	private String readerMaxSleep;
	private String script;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public Credentials getCredentials() {
		return credentials;
	}

	public void setCredentials(Credentials credentials) {
		this.credentials = credentials;
	}

	@JsonProperty("reader_min_sleep")
	public String getReaderMinSleep() {
		return readerMinSleep;
	}

	public void setReaderMinSleep(String readerMinSleep) {
		this.readerMinSleep = readerMinSleep;
	}

	@JsonProperty("reader_max_sleep")
	public String getReaderMaxSleep() {
		return readerMaxSleep;
	}

	public void setReaderMaxSleep(String readerMaxSleep) {
		this.readerMaxSleep = readerMaxSleep;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
}
