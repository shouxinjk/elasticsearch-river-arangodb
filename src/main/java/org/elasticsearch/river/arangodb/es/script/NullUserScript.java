package org.elasticsearch.river.arangodb.es.script;

import java.util.Map;

import net.swisstech.arangodb.model.wal.WalEvent;

/** this does nothing except return the context object and pretends some script has been executed, this is used in case the use didn't supply a script */
public class NullUserScript extends AbstractUserScript {

	@Override
	public Map<String, Object> executeScript(WalEvent event) {
		return createContextObject(event);
	}
}
