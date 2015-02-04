package org.elasticsearch.river.arangodb.testclient.config;

public class Meta {

	private String type = "arangodb";
	private Arangodb arangodb;
	private Index index;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Arangodb getArangodb() {
		return arangodb;
	}

	public void setArangodb(Arangodb arangodb) {
		this.arangodb = arangodb;
	}

	public Index getIndex() {
		return index;
	}

	public void setIndex(Index index) {
		this.index = index;
	}
}
