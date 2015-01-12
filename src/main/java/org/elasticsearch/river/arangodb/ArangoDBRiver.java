package org.elasticsearch.river.arangodb;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.arangodb.ArangoConstants.BULK_SIZE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.BULK_TIMEOUT_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.COLLECTION_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.CREDENTIALS_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.DB_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.DEFAULT_DB_HOST;
import static org.elasticsearch.river.arangodb.ArangoConstants.DEFAULT_DB_PORT;
import static org.elasticsearch.river.arangodb.ArangoConstants.DROP_COLLECTION_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.EXCLUDE_FIELDS_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.HOST_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.INDEX_OBJECT;
import static org.elasticsearch.river.arangodb.ArangoConstants.LAST_TICK_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.NAME_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.OPTIONS_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.PASSWORD_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.PORT_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;
import static org.elasticsearch.river.arangodb.ArangoConstants.SCRIPT_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.SCRIPT_TYPE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.THROTTLE_SIZE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.TYPE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.USER_FIELD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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
	private final String arangoAdminUser;
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

	private List<Slurper> slurpers = new ArrayList<Slurper>();
	private List<Thread> slurperThreads = new ArrayList<Thread>();
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

		for (String field : basicExcludeFields) {
			excludeFields.add(field);
		}

		if (settings.settings().containsKey(RIVER_TYPE)) {
			Map<String, Object> arangoSettings = (Map<String, Object>) settings.settings().get(RIVER_TYPE);

			arangoHost = XContentMapValues.nodeStringValue(arangoSettings.get(HOST_FIELD), DEFAULT_DB_HOST);
			arangoPort = XContentMapValues.nodeIntegerValue(arangoSettings.get(PORT_FIELD), DEFAULT_DB_PORT);

			// ArangoDB options
			if (arangoSettings.containsKey(OPTIONS_FIELD)) {
				Map<String, Object> arangoOptionsSettings = (Map<String, Object>) arangoSettings.get(OPTIONS_FIELD);

				dropCollection = XContentMapValues.nodeBooleanValue(arangoOptionsSettings.get(DROP_COLLECTION_FIELD), true);

				if (arangoOptionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) {
					Object excludeFieldsSettings = arangoOptionsSettings.get(EXCLUDE_FIELDS_FIELD);

					logger.info("excludeFieldsSettings: " + excludeFieldsSettings);

					if (XContentMapValues.isArray(excludeFieldsSettings)) {
						ArrayList<String> fields = (ArrayList<String>) excludeFieldsSettings;

						for (String field : fields) {
							logger.info("Field: " + field);
							excludeFields.add(field);
						}
					}
				}
			}
			else {
				dropCollection = true;
			}

			// Credentials
			if (arangoSettings.containsKey(CREDENTIALS_FIELD)) {
				Map<String, Object> credentials = (Map<String, Object>) arangoSettings.get(CREDENTIALS_FIELD);

				arangoAdminUser = XContentMapValues.nodeStringValue(credentials.get(USER_FIELD), null);
				arangoAdminPassword = XContentMapValues.nodeStringValue(credentials.get(PASSWORD_FIELD), null);
			}
			else {
				arangoAdminUser = "";
				arangoAdminPassword = "";
			}

			arangoDb = XContentMapValues.nodeStringValue(arangoSettings.get(DB_FIELD), riverName.name());
			arangoCollection = XContentMapValues.nodeStringValue(arangoSettings.get(COLLECTION_FIELD), riverName.name());

			if (arangoSettings.containsKey(SCRIPT_FIELD)) {
				String scriptLang = "js";

				if (arangoSettings.containsKey(SCRIPT_TYPE_FIELD)) {
					scriptLang = arangoSettings.get(SCRIPT_TYPE_FIELD).toString();
				}

				String scriptString = arangoSettings.get(SCRIPT_FIELD).toString();
				ScriptType scriptType = ScriptType.INLINE;
				script = scriptService.executable(scriptLang, scriptString, scriptType, newHashMap());
			}
			else {
				script = null;
			}
		}
		else {
			arangoHost = DEFAULT_DB_HOST;
			arangoPort = DEFAULT_DB_PORT;

			arangoDb = riverName.name();
			arangoCollection = riverName.name();
			arangoAdminUser = "";
			arangoAdminPassword = "";
			script = null;
			dropCollection = true;
		}

		if (settings.settings().containsKey(INDEX_OBJECT)) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX_OBJECT);

			indexName = XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), arangoDb);
			typeName = XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), arangoDb);
			bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE_FIELD), 100);

			if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
				bulkTimeout = TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"),
						TimeValue.timeValueMillis(10));
			} else {
				bulkTimeout = TimeValue.timeValueMillis(10);
			}

			throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get(THROTTLE_SIZE_FIELD), bulkSize * 5);

		}
		else {
			indexName = arangoDb;
			typeName = arangoDb;
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			throttleSize = bulkSize * 5;
		}

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

		Slurper slurper = new Slurper(lastProcessedTick, excludeFields, arangoCollection, arangoDb, arangoAdminUser, arangoAdminPassword, arangoHost, arangoPort, stream, this);
		Thread slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(slurper);

		slurpers.add(slurper);
		slurperThreads.add(slurperThread);

		for (Thread thread : slurperThreads) {
			logger.info("starting arangodb slurper [{}]", thread);
			thread.start();
		}

		indexer = new Indexer(this, client, typeName, indexName, arangoCollection, bulkTimeout, bulkSize, script, dropCollection, riverIndexName, riverName, stream);
		indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer").newThread(indexer);
		indexerThread.start();
		logger.info("starting arangodb indexer");
	}

	@Override
	public void close() {
		if (active) {
			logger.info("closing arangodb stream river");

			active = false;

			for (Slurper slurper : slurpers) {
				slurper.shutdown();
			}

			for (Thread thread : slurperThreads) {
				thread.interrupt();
				logger.info("stopping arangodb slurper [{}]", thread);
			}

			indexerThread.interrupt();
			logger.info("stopping arangodb indexer");
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
