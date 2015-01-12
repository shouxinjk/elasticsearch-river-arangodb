package org.elasticsearch.river.arangodb;

import static ch.bind.philib.lang.ThreadUtil.interruptAndJoin;
import static org.elasticsearch.river.arangodb.ArangoConstants.LAST_TICK_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import net.swisstech.swissarmyknife.io.Closeables;
import net.swisstech.swissarmyknife.lang.Threads;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

public class ArangoDBRiver extends AbstractRiverComponent implements River {

	private final Client client;

	private final String riverIndexName;

	private Slurper slurper;
	private Thread slurperThread;
	private Indexer indexer;
	private Thread indexerThread;

	private final BlockingQueue<Map<String, Object>> stream;

	private String arangoHost;
	private int arangoPort;

	private final ArangoDbConfig config;

	@Inject
	public ArangoDBRiver( //
		final RiverName riverName, //
		final RiverSettings settings, //
		@RiverIndexName final String riverIndexName, //
		final Client client, //
		final ScriptService scriptService, //
		final ArangoDbConfig config //
	) throws ArangoException {

		super(riverName, settings);
		this.config = config;

		if (logger.isDebugEnabled()) {
			logger.debug("Prefix: [{}] - name: [{}]", logger.getPrefix(), logger.getName());
			logger.debug("River settings: [{}]", settings.settings());
		}

		this.riverIndexName = riverIndexName;
		this.client = client;


		if (config.getIndexThrottleSize() == -1) {
			stream = new LinkedTransferQueue<Map<String, Object>>();
		}
		else {
			stream = new ArrayBlockingQueue<Map<String, Object>>(config.getIndexThrottleSize());
		}
	}

	@Override
	public void start() {
		logger.info("using arangodb server(s): host [{}], port [{}]", arangoHost, arangoPort);
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

		String lastProcessedTick = fetchLastTick(config.getArangodbCollection());

		// TODO let guice construct the slurper
		slurper = new Slurper(config, lastProcessedTick, stream);
		slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(slurper);

		// TODO let guice construct the indexer
		indexer = new Indexer(config, client, riverIndexName, riverName, stream);
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

	private String fetchLastTick(final String namespace) {
		logger.info("fetching last tick for collection {}", namespace);

		GetResponse stateResponse = client.prepareGet(riverIndexName, riverName.getName(), namespace).execute().actionGet();

		if (stateResponse.isExists()) {
			Map<String, Object> indexState = (Map<String, Object>) stateResponse.getSourceAsMap().get(RIVER_TYPE);

			if (indexState != null) {
				try {
					String lastTick = indexState.get(LAST_TICK_FIELD).toString();
					logger.info("found last tick for collection {}: {}", namespace, lastTick);
					return lastTick;

				}
				catch (Exception ex) {
					logger.error("error fetching last tick for collection {}: {}", namespace, ex);
				}
			}
			else {
				logger.info("fetching last tick: indexState is null");
			}
		}

		return null;
	}
}
