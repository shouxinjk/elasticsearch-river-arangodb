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
		if (isBlank(user) && isBlank(pass)) {
			return new WalClient(baseUrl);
		}
		else {
			return new WalClient(baseUrl, user, pass);
		}
	}

	@Provides
	@Singleton
	@Named("arangodb_river_slurper_threadfactory")
	public ThreadFactory getSlurperThreadFactory(RiverSettings settings) {
		return EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper");
	}

	@Provides
	@Singleton
	@Named("arangodb_river_indexer_threadfactory")
	public ThreadFactory getIndexerThreadFactory(RiverSettings settings) {
		return EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer");
	}
}
