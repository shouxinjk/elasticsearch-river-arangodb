package org.elasticsearch.river.arangodb;

public interface ArangoConstants {

	String RIVER_TYPE = "arangodb";
	String NAME_FIELD = "name";
	String LAST_TICK_FIELD = "_last_tick";
	String REPLOG_ENTRY_UNDEFINED = "undefined";
	String REPLOG_FIELD_KEY = "key";
	String REPLOG_FIELD_TICK = "tick";
	String STREAM_FIELD_OPERATION = "op";

	String HTTP_PROTOCOL = "http";
	String HTTP_HEADER_CHECKMORE = "x-arango-replication-checkmore";
	String HTTP_HEADER_LASTINCLUDED = "x-arango-replication-lastincluded";
}
