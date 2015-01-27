package org.elasticsearch.river.arangodb;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
	@Named("arangodb_river_httpclient")
	public CloseableHttpClient getHttpClient(ArangoDbConfig config) {

		// if the temporary objects below are needed elsewhere, one can split
		// this method into multiple provider methods and reuse the instances

		String host = config.getArangodbHost();
		int port = config.getArangodbPort();
		AuthScope scope = new AuthScope(host, port);

		String user = config.getArangodbCredentialsUsername();
		String pass = config.getArangodbCredentialsPassword();
		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(user, pass);

		CredentialsProvider cprov = new BasicCredentialsProvider();
		cprov.setCredentials(scope, creds);

		return HttpClients //
			.custom() //
			.setDefaultCredentialsProvider(cprov) //
			.build();
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
