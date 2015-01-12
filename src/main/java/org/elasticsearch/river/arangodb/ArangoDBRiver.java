package org.elasticsearch.river.arangodb;

import static ch.bind.philib.lang.ThreadUtil.interruptAndJoin;
import net.swisstech.swissarmyknife.io.Closeables;
import net.swisstech.swissarmyknife.lang.Threads;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

public class ArangoDBRiver extends AbstractRiverComponent implements River {


	private final Client client;
	private final ArangoDbConfig config;
	private Slurper slurper;
	private Indexer indexer;

	private Thread slurperThread;
	private Thread indexerThread;

	@Inject
	public ArangoDBRiver( //
	final RiverName riverName, //
		final RiverSettings settings, //
		final Client client, //
		final ScriptService scriptService, //
		final ArangoDbConfig config, //
		final Slurper slurper, //
		final Indexer indexer //
	) throws ArangoException {

		super(riverName, settings);
		this.client = client;
		this.config = config;
		this.slurper = slurper;
		this.indexer = indexer;

		if (logger.isDebugEnabled()) {
			logger.debug("Prefix: [{}] - name: [{}]", logger.getPrefix(), logger.getName());
			logger.debug("River settings: [{}]", settings.settings());
		}
	}

	@Override
	public void start() {
		logger.info("using arangodb server(s): host [{}], port [{}]", config.getArangodbHost(), config.getArangodbPort());
		logger.info("starting arangodb stream. options: throttlesize [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]", //
			config.getIndexThrottleSize(), //
			config.getArangodbDatabase(), //
			config.getArangodbCollection(), //
			config.getArangodbScript(), //
			config.getIndexName(), //
			config.getIndexType() //
			);

		try {
			client.admin().indices().prepareCreate(config.getIndexName()).execute().actionGet();
		}
		catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// ok
			}
			else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ..
			}
			else {
				logger.warn("failed to create index [{}], disabling river...", e, config.getIndexName());
				return;
			}
		}

		// TODO let guice construct the slurper
		slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(slurper);

		// TODO let guice construct the indexer
		indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer").newThread(indexer);

		slurperThread.start();
		indexerThread.start();

		logger.info("started arangodb river");
	}

	@Override
	public void close() {
		logger.info("closing arangodb stream river");

		Closeables.close(slurper);
		Closeables.close(indexer);

		Threads.sleepFor(100);

		interruptAndJoin(slurperThread, 50);
		interruptAndJoin(indexerThread, 50);
	}
}
