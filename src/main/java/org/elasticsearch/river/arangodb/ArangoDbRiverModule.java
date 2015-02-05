package org.elasticsearch.river.arangodb;

import static net.swisstech.swissarmyknife.lang.Strings.isBlank;

import java.util.concurrent.ThreadFactory;

import net.swisstech.arangodb.WalClient;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Provides;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.es.script.ConcreteUserScript;
import org.elasticsearch.river.arangodb.es.script.NullUserScript;
import org.elasticsearch.river.arangodb.es.script.UserScript;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ArangoDbRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(ArangoDbRiver.class).asEagerSingleton();
	}

	@Provides
	@Singleton
	public WalClient provideWalClient(ArangoDbConfig config) {
		String host = config.getArangodbHost();
		int port = config.getArangodbPort();
		String db = config.getArangodbDatabase();

		String baseUrl = String.format("http://%s:%d/_db/%s", host, port, db);

		String user = config.getArangodbCredentialsUsername();
		String pass = config.getArangodbCredentialsPassword();
		if (user == null && pass == null) {
			return new WalClient(baseUrl);
		}
		else {
			return new WalClient(baseUrl, user, pass);
		}
	}

	@Provides
	@Singleton
	public UserScript provideUserScript(ArangoDbConfig config, ScriptService scriptService) {
		String scriptString = config.getArangodbScript();
		if (isBlank(scriptString)) {
			return new NullUserScript();
		}

		String scriptType = config.getArangodbScripttype();
		if (isBlank(scriptType)) {
			return new NullUserScript();
		}

		CompiledScript compiled = scriptService.compile(scriptType, scriptString, ScriptType.INLINE);
		return new ConcreteUserScript(scriptService, compiled);
	}

	@Provides
	@Singleton
	@Named("arangodb_river_walReaderRunnable_threadfactory")
	public ThreadFactory getSlurperThreadFactory(RiverSettings settings) {
		return EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_walreader");
	}

	@Provides
	@Singleton
	@Named("arangodb_river_indexWriterRunnable_threadfactory")
	public ThreadFactory getIndexerThreadFactory(RiverSettings settings) {
		return EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer");
	}
}
