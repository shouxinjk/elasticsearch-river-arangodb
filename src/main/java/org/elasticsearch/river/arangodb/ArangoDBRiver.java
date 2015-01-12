package org.elasticsearch.river.arangodb;

import static ch.bind.philib.lang.ThreadUtil.interruptAndJoin;
import static java.util.Arrays.asList;
import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.arangodb.ArangoConstants.LAST_TICK_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ArangoDBRiver extends AbstractRiverComponent implements River {

	private final Client client;

	private final String riverIndexName;

	private final String arangoDb;
	private final String arangoCollection;
	private final String arangoAdminUsername;
	private final String arangoAdminPassword;

	private final String indexName;
	private final String typeName;
	private final int bulkSize;
	private final TimeValue bulkTimeout;
	private final int throttleSize;
	private final boolean dropCollection;

	private final ArrayList<String> basicExcludeFields = new ArrayList<String>(asList("_id", "_key", "_rev"));
	private Set<String> excludeFields = new HashSet<String>();

	private final ExecutableScript script;

	private Slurper slurper;
	private Thread slurperThread;
	private Indexer indexer;
	private Thread indexerThread;

	private volatile boolean active = true;

	private final BlockingQueue<Map<String, Object>> stream;

	private String arangoHost;
	private int arangoPort;

	@Inject
	public ArangoDBRiver( //
		final RiverName riverName, //
		final RiverSettings settings, //
		@RiverIndexName final String riverIndexName, //
		final Client client, //
		final ScriptService scriptService //
	) throws ArangoException {

		super(riverName, settings);

		if (logger.isDebugEnabled()) {
			logger.debug("Prefix: [{}] - name: [{}]", logger.getPrefix(), logger.getName());
			logger.debug("River settings: [{}]", settings.settings());
		}

		this.riverIndexName = riverIndexName;
		this.client = client;

		RiverSettingsWrapper rsw = new RiverSettingsWrapper(settings);

		arangoHost = rsw.getString("arangodb.host", "localhost");
		arangoPort = rsw.getInt("arangodb.port", 8529);

		dropCollection = rsw.getBool("arangodb.options.drop_collection", true);

		excludeFields.addAll(basicExcludeFields);
		excludeFields.addAll(rsw.getList("arangodb.options.exclude_fields", new ArrayList<String>()));

		arangoAdminUsername = rsw.getString("arangodb.credentials.username", "");
		arangoAdminPassword = rsw.getString("arangodb.credentials.password", "");

		arangoDb = rsw.getString("arangodb.db", riverName.name());
		arangoCollection = rsw.getString("arangodb.collection", riverName.name());

		String scriptString = rsw.getString("arangodb.script", null);
		String scriptLang = rsw.getString("arangodb.scriptType", "js");

		ScriptType scriptType = ScriptType.INLINE;
		script = scriptService.executable(scriptLang, scriptString, scriptType, newHashMap());

		indexName = rsw.getString("index.name", riverName.name());
		typeName = rsw.getString("index.type", riverName.name());
		bulkSize = rsw.getInt("index.bulk_size", 100);

		String bulkTimeoutString = rsw.getString("index.bulk_timeout", "10ms");
		bulkTimeout = TimeValue.parseTimeValue(bulkTimeoutString, null);

		throttleSize = rsw.getInt("index.throttle_size", bulkSize * 5);

		if (throttleSize == -1) {
			stream = new LinkedTransferQueue<Map<String, Object>>();
		}
		else {
			stream = new ArrayBlockingQueue<Map<String, Object>>(throttleSize);
		}
	}

	@Override
	public void start() {
		logger.info("using arangodb server(s): host [{}], port [{}]", arangoHost, arangoPort);
		logger.info("starting arangodb stream. options: throttlesize [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]", throttleSize, arangoDb, arangoCollection, script, indexName, typeName);

		try {
			client.admin().indices().prepareCreate(indexName).execute().actionGet();
		}
		catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// ok
			}
			else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ..
			}
			else {
				logger.warn("failed to create index [{}], disabling river...", e, indexName);
				return;
			}
		}

		String lastProcessedTick = fetchLastTick(arangoCollection);

		slurper = new Slurper(lastProcessedTick, excludeFields, arangoCollection, arangoDb, arangoAdminUsername, arangoAdminPassword, arangoHost, arangoPort, stream, this);
		slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(slurper);

		indexer = new Indexer(this, client, typeName, indexName, arangoCollection, bulkTimeout, bulkSize, script, dropCollection, riverIndexName, riverName, stream);
		indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer").newThread(indexer);

		slurperThread.start();
		indexerThread.start();

		logger.info("started arangodb river");
	}

	@Override
	public void close() {
		if (active) {
			logger.info("closing arangodb stream river");

			active = false;

			slurper.shutdown();

			// indexer uses ArangoDbRiver.isActive() and has no shutdown yet
			// indexer.shutdown();

			interruptAndJoin(slurperThread);
			interruptAndJoin(indexerThread);
		}
	}

	public boolean isActive() {
		return active;
	}

	private String fetchLastTick(final String namespace) {
		String lastTick = null;

		logger.info("fetching last tick for collection {}", namespace);

		GetResponse stateResponse = client
				.prepareGet(riverIndexName, riverName.getName(), namespace)
				.execute().actionGet();

		if (stateResponse.isExists()) {
			Map<String, Object> indexState = (Map<String, Object>) stateResponse.getSourceAsMap().get(RIVER_TYPE);

			if (indexState != null) {
				try {
					lastTick = indexState.get(LAST_TICK_FIELD).toString();

					logger.info("found last tick for collection {}: {}", namespace, lastTick);

				}
				catch (Exception ex) {
					logger.error("error fetching last tick for collection {}: {}", namespace, ex);
				}
			}
			else {
				logger.info("fetching last tick: indexState is null");
			}
		}

		return lastTick;
	}
}
