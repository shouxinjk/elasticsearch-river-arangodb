package org.elasticsearch.river.arangodb;

/** see https://github.com/triAGENS/ArangoDB/blob/master/arangod/VocBase/replication-common.h */
public enum OpType {

	INSERT,
	UPDATE,
	DELETE;
}
