package org.elasticsearch.river.arangodb.wal;

public enum StateName {

	COLLECTION_CHECK, //
	COLLECTION_MISSING, //
	DROP_COLLECTION, //
	ENQUEUE, //
	READ_WAL, //
	SLEEP;
}
