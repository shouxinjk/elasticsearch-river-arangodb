package org.elasticsearch.plugin.river.arangodb;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.arangodb.ArangoDbRiverModule;

public class ArangoDbRiverPlugin extends AbstractPlugin {

	public static final String NAME = "river-arangodb";
	public static final String DESC = "ArangoDB River Plugin";

	@Inject
	public ArangoDbRiverPlugin() {}

	@Override
	public String name() {
		return NAME;
	}

	@Override
	public String description() {
		return DESC;
	}

	public void onModule(RiversModule module) {
		module.registerRiver("arangodb", ArangoDbRiverModule.class);
	}
}
