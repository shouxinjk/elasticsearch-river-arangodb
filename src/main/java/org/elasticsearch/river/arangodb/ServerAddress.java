package org.elasticsearch.river.arangodb;

public class ServerAddress {

	private String host;
	private int port;

	public ServerAddress(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
}
