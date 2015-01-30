package org.elasticsearch.river.arangodb.es;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.script.ExecutableScript;

@Singleton
public class WalEventConverter {

	private final ExecutableScript script;

	@Inject
	public WalEventConverter(ArangoDbConfig config) {
		script = config.getArangodbScript();
	}

	public void
}
