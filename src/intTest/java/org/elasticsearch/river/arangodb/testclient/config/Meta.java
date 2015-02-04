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

	public static Meta create(String db, String collection, String host, int port, String indexName, String indexType) {

		Arangodb arangodb = new Arangodb();
		arangodb.setCollection(collection);
		arangodb.setCredentials(null);
		arangodb.setDb(db);
		arangodb.setHost(host);
		arangodb.setPort(port);
		arangodb.setReaderMaxSleep("5000ms");
		arangodb.setReaderMinSleep("100ms");
		arangodb.setScript(null);

		Index index = new Index();
		index.setBulkSize(5000);
		index.setBulkTimeout("50ms");
		index.setName(indexName);
		index.setType(indexType);

		Meta meta = new Meta();
		meta.setArangodb(arangodb);
		meta.setIndex(index);

		return meta;
	}
}
