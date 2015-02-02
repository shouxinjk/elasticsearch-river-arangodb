package org.elasticsearch.river.arangodb.es.script;

import java.util.Map;

import net.swisstech.arangodb.model.wal.WalEvent;

public interface UserScript {

	/**
	 * Execute the script supplied when the river was activated
	 * @param event The WAL Event
	 * @return The Context Object supplied to the script, unwrapped after execution
	 */
	Map<String, Object> executeScript(WalEvent event);
}
