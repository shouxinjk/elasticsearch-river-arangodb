package org.elasticsearch.river.arangodb.wal;

public enum StateName {

	COLLECTION_CHECK, //
	ENQUEUE, //
	READ_WAL, //
	SLEEP;
}
