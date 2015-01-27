package org.elasticsearch.river.arangodb;

import static net.swisstech.swissarmyknife.lang.Strings.isBlank;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
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
		String baseUrl = String.format("http://%s:%d", host, port);
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
	@Named("arangodb_river_eventstream")
	public BlockingQueue<Map<String, Object>> getStream(ArangoDbConfig config) {
		int throttle = config.getIndexThrottleSize();
		if (throttle == -1) {
			return new LinkedTransferQueue<Map<String, Object>>();
		}
		return new ArrayBlockingQueue<Map<String, Object>>(throttle);
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
