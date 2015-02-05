package org.elasticsearch.river.arangodb.wal;

public interface State {

	void execute();
}
