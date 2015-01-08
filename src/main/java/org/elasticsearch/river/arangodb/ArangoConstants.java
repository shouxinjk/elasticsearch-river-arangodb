package org.elasticsearch.river.arangodb;

public interface ArangoConstants {

	int DEFAULT_DB_PORT = 8529;

	String RIVER_TYPE = "arangodb";
	String DB_FIELD = "db";
	String HOST_FIELD = "host";
	String PORT_FIELD = "port";
	String OPTIONS_FIELD = "options";
	String DROP_COLLECTION_FIELD = "drop_collection";
	String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	String CREDENTIALS_FIELD = "credentials";
	String USER_FIELD = "username";
	String PASSWORD_FIELD = "password";
	String SCRIPT_FIELD = "script";
	String SCRIPT_TYPE_FIELD = "scriptType";
	String COLLECTION_FIELD = "collection";
	String INDEX_OBJECT = "index";
	String NAME_FIELD = "name";
	String TYPE_FIELD = "type";
	String DEFAULT_DB_HOST = "localhost";
	String THROTTLE_SIZE_FIELD = "throttle_size";
	String BULK_SIZE_FIELD = "bulk_size";
	String BULK_TIMEOUT_FIELD = "bulk_timeout";
	String LAST_TICK_FIELD = "_last_tick";
	String REPLOG_ENTRY_UNDEFINED = "undefined";
	String REPLOG_FIELD_KEY = "key";
	String REPLOG_FIELD_TICK = "tick";
	String STREAM_FIELD_OPERATION = "op";

	String HTTP_PROTOCOL = "http";
	String HTTP_HEADER_CHECKMORE = "x-arango-replication-checkmore";
	String HTTP_HEADER_LASTINCLUDED = "x-arango-replication-lastincluded";
}
