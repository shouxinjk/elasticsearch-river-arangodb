package org.elasticsearch.river.arangodb.es.script;

import java.util.HashMap;
import java.util.Map;

import net.swisstech.arangodb.model.wal.WalEvent;
import net.swisstech.arangodb.model.wal.WalEventType;

public abstract class AbstractUserScript implements UserScript {

	/**
	 * creates the context object to execute the user supplied script on the WAL event. the script has full access to this object under the 'ctx' variable and
	 * can modify everything and influence the behaviour of the indexer or even modify the data to be indexed.
	 * @param event the WAL event for which to create the context object
	 * @return the context object for the script
	 */
	public Map<String, Object> createContextObject(WalEvent event) {
		Map<String, Object> vars = new HashMap<>();
		vars.put("id", event.getKey());
		vars.put("doc", event.getData());
		vars.put("rev", event.getRev());
		vars.put("tick", event.getTick());

		WalEventType type = event.getType();
		vars.put("opid", type.getId());
		vars.put("opname", type.name());

		if (type == WalEventType.REPLICATION_MARKER_DOCUMENT || type == WalEventType.REPLICATION_MARKER_EDGE) {
			vars.put("operation", "UPDATE");
		}
		else if (type == WalEventType.REPLICATION_MARKER_REMOVE) {
			vars.put("operation", "DELETE");
		}
		else {
			vars.put("operation", "SKIP");
		}

		return vars;
	}
}
