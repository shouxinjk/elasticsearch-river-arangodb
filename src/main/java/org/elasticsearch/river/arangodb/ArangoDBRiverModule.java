package org.elasticsearch.river.arangodb;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Provides;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverSettings;

public class ArangoDBRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(ArangoDBRiver.class).asEagerSingleton();
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
