package org.elasticsearch.river.arangodb;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_ENTRY_UNDEFINED;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_KEY;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_TICK;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;
import static org.elasticsearch.river.arangodb.ArangoConstants.SCRIPT_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.SCRIPT_TYPE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.STREAM_FIELD_OPERATION;
import static org.elasticsearch.river.arangodb.ArangoConstants.THROTTLE_SIZE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.TYPE_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.USER_FIELD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
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

	private final List<ServerAddress> arangoServers = new ArrayList<ServerAddress>();
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

	private volatile List<Slurper> slurpers = new ArrayList<Slurper>();
	private volatile List<Thread> slurperThreads = new ArrayList<Thread>();
	private volatile Thread indexerThread;
	private volatile boolean active = true;

	private final BlockingQueue<Map<String, Object>> stream;

	private String arangoHost;
	private int arangoPort;


	@Inject
	public ArangoDBRiver(final RiverName riverName, final RiverSettings settings,
			@RiverIndexName final String riverIndexName, final Client client, final ScriptService scriptService) throws ArangoException {
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

			arangoServers.add(new ServerAddress(arangoHost, arangoPort));

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
			} else {
				dropCollection = true;
			}

			// Credentials
			if (arangoSettings.containsKey(CREDENTIALS_FIELD)) {
				Map<String, Object> credentials = (Map<String, Object>) arangoSettings.get(CREDENTIALS_FIELD);

				arangoAdminUser = XContentMapValues.nodeStringValue(credentials.get(USER_FIELD), null);
				arangoAdminPassword = XContentMapValues.nodeStringValue(credentials.get(PASSWORD_FIELD), null);

			} else {
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
			} else {
				script = null;
			}
		} else {
			arangoHost = DEFAULT_DB_HOST;
			arangoPort = DEFAULT_DB_PORT;

			arangoServers.add(new ServerAddress(arangoHost, arangoPort));

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

		} else {
			indexName = arangoDb;
			typeName = arangoDb;
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			throttleSize = bulkSize * 5;
		}

		if (throttleSize == -1) {
			stream = new LinkedTransferQueue<Map<String, Object>>();
		} else {
			stream = new ArrayBlockingQueue<Map<String, Object>>(throttleSize);
		}
	}

	@Override
	public void start() {
		for (ServerAddress server : arangoServers) {
			logger.info("using arangodb server(s): host [{}], port [{}]", server.getHost(), server.getPort());
		}

		logger.info("starting arangodb stream. options: throttlesize [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
				throttleSize,
				arangoDb,
				arangoCollection,
				script,
				indexName,
				typeName);

		try {
			client.admin().indices().prepareCreate(indexName).execute().actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// ok
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ..
			} else {
				logger.warn("failed to create index [{}], disabling river...", e, indexName);
				return;
			}
		}

		String lastProcessedTick = fetchLastTick(arangoCollection);

		Slurper slurper = new Slurper(lastProcessedTick, excludeFields, arangoCollection, arangoDb, arangoAdminUser, arangoAdminPassword, arangoServers, stream, this);
		Thread slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(slurper);

		slurpers.add(slurper);
		slurperThreads.add(slurperThread);

		for (Thread thread : slurperThreads) {
			logger.info("starting arangodb slurper [{}]", thread);
			thread.start();
		}

		indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer").newThread(new Indexer());
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

				} catch (Exception ex) {
					logger.error("error fetching last tick for collection {}: {}", namespace, ex);
				}
			} else {
				logger.info("fetching last tick: indexState is null");
			}
		}

		return lastTick;
	}



	private class Indexer implements Runnable {
		private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
		private int deletedDocuments = 0;
		private int insertedDocuments = 0;
		private int updatedDocuments = 0;
		private StopWatch sw;

		@Override
		public void run() {
			logger.info("=== river-arangodb indexer running ... ===");

			while (active) {
				sw = new StopWatch().start();

				deletedDocuments = 0;
				insertedDocuments = 0;
				updatedDocuments = 0;

				try {
					String lastTick = null;
					BulkRequestBuilder bulk = client.prepareBulk();

					// 1. Attempt to fill as much of the bulk request as possible
					Map<String, Object> data = stream.take();
					lastTick = updateBulkRequest(bulk, data);

					while ((data = stream.poll(bulkTimeout.millis(), MILLISECONDS)) != null) {
						lastTick = updateBulkRequest(bulk, data);

						if (bulk.numberOfActions() >= bulkSize) {
							break;
						}
					}

					// 2. Update the Tick
					if (lastTick != null) {
						updateLastTick(arangoCollection, lastTick, bulk);
					}

					// 3. Execute the bulk requests
					try {
						BulkResponse response = bulk.execute().actionGet();

						if (response.hasFailures()) {
							logger.warn("failed to execute" + response.buildFailureMessage());
						}
					} catch (Exception e) {
						logger.warn("failed to execute bulk", e);
					}

				} catch (InterruptedException e) {
					if (logger.isDebugEnabled()) {
						logger.debug("river-arangodb indexer interrupted");
					}

					Thread.currentThread().interrupt();
				}

				logStatistics();
			}
		}

		private String updateBulkRequest(final BulkRequestBuilder bulk, Map<String, Object> data) {
			String replogTick = (String) data.get(REPLOG_FIELD_TICK);
			OpType operation = (OpType) data.get(STREAM_FIELD_OPERATION);
			String objectId = "";

			if (data.get(REPLOG_FIELD_KEY) != null) {
				objectId = (String) data.get(REPLOG_FIELD_KEY);
			}

			data.remove(REPLOG_FIELD_TICK);
			data.remove(STREAM_FIELD_OPERATION);

			if (logger.isDebugEnabled()) {
				logger.debug("updateBulkRequest for id: [{}], operation: [{}]", objectId, operation);
				logger.debug("data: [{}]", data);
			}

			Map<String, Object> ctx = null;

			try {
				ctx = XContentFactory.xContent(XContentType.JSON).createParser("{}").mapAndClose();
			} catch (IOException e) {
				logger.warn("failed to parse {}", e);
			}

			if (script != null) {
				if (ctx != null) {
					ctx.put("doc", data);
					ctx.put("operation", operation);

					if (!objectId.isEmpty()) {
						ctx.put("id", objectId);
					}

					if (logger.isDebugEnabled()) {
						logger.debug("Context before script executed: {}", ctx);
					}

					script.setNextVar("ctx", ctx);

					try {
						script.run();
						ctx = (Map<String, Object>) script.unwrap(ctx);
					} catch (Exception e) {
						logger.warn("failed to script process {}, ignoring", e, ctx);
					}

					if (logger.isDebugEnabled()) {
						logger.debug("Context after script executed: {}", ctx);
					}

					if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
						logger.debug("From script ignore document id: {}", objectId);
						return replogTick;
					}

					if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
						ctx.put("operation", "DELETE");
					}

					if (ctx.containsKey("doc")) {
						data = (Map<String, Object>) ctx.get("doc");
						logger.debug("From script document: {}", data);
					}

					if (ctx.containsKey("operation")) {
						operation = OpType.valueOf(ctx.get("operation").toString());
						logger.debug("From script operation: {}", operation);
					}
				}
			}

			try {
				String index = extractIndex(ctx);
				String type = extractType(ctx);
				String parent = extractParent(ctx);
				String routing = extractRouting(ctx);

				if (logger.isDebugEnabled()) {
					logger.debug("Operation: {} - index: {} - type: {} - routing: {} - parent: {}",
							operation, index, type, routing, parent);
				}

				if (operation == OpType.INSERT) {
					if (logger.isDebugEnabled()) {
						logger.debug("Insert operation - id: {}", operation, objectId);
					}

					bulk.add(indexRequest(index).type(type).id(objectId)
							.source(build(data, objectId)).routing(routing)
							.parent(parent));

					insertedDocuments++;
				}
				else if (operation == OpType.UPDATE) {
					if (logger.isDebugEnabled()) {
						logger.debug("Update operation - id: {}", objectId);
					}

					bulk.add(new DeleteRequest(index, type, objectId).routing(routing).parent(parent));
					bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));

					updatedDocuments++;
				}
				else if (operation == OpType.DELETE) {
					if (logger.isDebugEnabled()) {
						logger.debug("Delete operation - id: {}, data [{}]", objectId, data);
					}

					if (REPLOG_ENTRY_UNDEFINED.equals(objectId) && data.get(NAME_FIELD).equals(arangoCollection)) {
						if (dropCollection) {
							logger.info("Drop collection request [{}], [{}]", index, type);

							bulk.request().requests().clear();
							client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();

							deletedDocuments = 0;
							updatedDocuments = 0;
							insertedDocuments = 0;

							logger.info("Delete request for index / type [{}] [{}] successfully executed.", index, type);
						} else {
							logger.info("Ignore drop collection request [{}], [{}]. The option has been disabled.", index, type);
						}
					} else {
						logger.info("Delete request [{}], [{}], [{}]", index, type, objectId);

						bulk.add(new DeleteRequest(index, type, objectId).routing(routing).parent(parent));

						deletedDocuments++;
					}
				}
			} catch (IOException e) {
				logger.warn("failed to parse {}", e, data);
			}

			return replogTick;
		}

		private void updateLastTick(final String namespace, final String tick, final BulkRequestBuilder bulk) {
			try {
				bulk.add(indexRequest(riverIndexName)
					.type(riverName.getName())
					.id(namespace)
					.source(jsonBuilder().startObject().startObject(RIVER_TYPE)
						.field(LAST_TICK_FIELD, tick)
						.endObject().endObject()));
			} catch (IOException e) {
				logger.error("error updating last Tick for collection {}", namespace);
			}
		}

		private XContentBuilder build(final Map<String, Object> data, final String objectId) throws IOException {
			return XContentFactory.jsonBuilder().map(data);
		}

		private String extractParent(Map<String, Object> ctx) {
			return (String) ctx.get("_parent");
		}

		private String extractRouting(Map<String, Object> ctx) {
			return (String) ctx.get("_routing");
		}

		private String extractType(Map<String, Object> ctx) {
			String type = (String) ctx.get("_type");

			if (type == null) {
				type = typeName;
			}

			return type;
		}

		private String extractIndex(Map<String, Object> ctx) {
			String index = (String) ctx.get("_index");

			if (index == null) {
				index = indexName;
			}

			return index;
		}

		private void logStatistics() {
			long totalDocuments = deletedDocuments + insertedDocuments;
			long totalTimeInSeconds = sw.stop().totalTime().seconds();
			long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;

			logger.info("Indexed {} documents, {} insertions {}, updates, {} deletions, {} documents per second",
					totalDocuments, insertedDocuments, updatedDocuments, deletedDocuments, totalDocumentsPerSecond);
		}
	}
}
