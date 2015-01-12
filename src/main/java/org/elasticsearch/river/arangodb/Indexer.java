package org.elasticsearch.river.arangodb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.river.arangodb.ArangoConstants.LAST_TICK_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.NAME_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_ENTRY_UNDEFINED;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_KEY;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_TICK;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;
import static org.elasticsearch.river.arangodb.ArangoConstants.STREAM_FIELD_OPERATION;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.script.ExecutableScript;

public class Indexer implements Runnable, Closeable {

	private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

	private volatile boolean keepRunning = true;

	private int deletedDocuments = 0;
	private int insertedDocuments = 0;
	private int updatedDocuments = 0;

	private final ArangoDbConfig config;
	private final Client client;
	private final String riverIndexName;
	private final RiverName riverName;
	private final BlockingQueue<Map<String, Object>> stream;

	public Indexer(ArangoDbConfig config, Client client, String riverIndexName, RiverName riverName, BlockingQueue<Map<String, Object>> stream) {
		this.config = config;
		this.client = client;
		this.riverIndexName = riverIndexName;
		this.riverName = riverName;
		this.stream = stream;
	}

	@Override
	public void run() {
		logger.info("=== river-arangodb indexer running ... ===");

		while (keepRunning) {
			StopWatch sw = new StopWatch().start();

			deletedDocuments = 0;
			insertedDocuments = 0;
			updatedDocuments = 0;

			try {
				String lastTick = null;
				BulkRequestBuilder bulk = client.prepareBulk();

				// 1. Attempt to fill as much of the bulk request as possible
				Map<String, Object> data = stream.take();
				lastTick = updateBulkRequest(bulk, data);

				while ((data = stream.poll(config.getIndexBulkTimeout().millis(), MILLISECONDS)) != null) {
					lastTick = updateBulkRequest(bulk, data);
					if (bulk.numberOfActions() >= config.getIndexBulkSize()) {
						break;
					}
				}

				// 2. Update the Tick
				if (lastTick != null) {
					updateLastTick(config.getArangodbCollection(), lastTick, bulk);
				}

				// 3. Execute the bulk requests
				try {
					BulkResponse response = bulk.execute().actionGet();
					if (response.hasFailures()) {
						logger.warn("failed to execute" + response.buildFailureMessage());
					}
				}
				catch (Exception e) {
					logger.warn("failed to execute bulk", e);
				}
			}
			catch (InterruptedException e) {
				if (logger.isDebugEnabled()) {
					logger.debug("river-arangodb indexer interrupted");
				}
				Thread.currentThread().interrupt();
			}
			logStatistics(sw);
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
		}
		catch (IOException e) {
			logger.warn("failed to parse {}", e);
		}

		ExecutableScript script = config.getArangodbScript();
		if (script != null && ctx != null) {
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
			}
			catch (Exception e) {
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

		try {
			String index = extractIndex(ctx);
			String type = extractType(ctx);
			String parent = extractParent(ctx);
			String routing = extractRouting(ctx);

			if (logger.isDebugEnabled()) {
				logger.debug("Operation: {} - index: {} - type: {} - routing: {} - parent: {}", operation, index, type, routing, parent);
			}

			if (operation == OpType.INSERT) {
				if (logger.isDebugEnabled()) {
					logger.debug("Insert operation - id: {}", operation, objectId);
				}

				bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));

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

				if (REPLOG_ENTRY_UNDEFINED.equals(objectId) && data.get(NAME_FIELD).equals(config.getArangodbCollection())) {
					if (config.isArangodbOptionsDropcollection()) {
						logger.info("Drop collection request [{}], [{}]", index, type);

						bulk.request().requests().clear();
						client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet();

						deletedDocuments = 0;
						updatedDocuments = 0;
						insertedDocuments = 0;

						logger.info("Delete request for index / type [{}] [{}] successfully executed.", index, type);
					}
					else {
						logger.info("Ignore drop collection request [{}], [{}]. The option has been disabled.", index, type);
					}
				}
				else {
					logger.info("Delete request [{}], [{}], [{}]", index, type, objectId);

					bulk.add(new DeleteRequest(index, type, objectId).routing(routing).parent(parent));

					deletedDocuments++;
				}
			}
		}
		catch (IOException e) {
			logger.warn("failed to parse {}", e, data);
		}

		return replogTick;
	}

	private void updateLastTick(final String namespace, final String tick, final BulkRequestBuilder bulk) {
		try {
			bulk.add(indexRequest(riverIndexName).type(riverName.getName()).id(namespace).source(jsonBuilder().startObject().startObject(RIVER_TYPE).field(LAST_TICK_FIELD, tick).endObject().endObject()));
		}
		catch (IOException e) {
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
			return config.getIndexType();
		}
		return type;
	}

	private String extractIndex(Map<String, Object> ctx) {
		String index = (String) ctx.get("_index");
		if (index == null) {
			return config.getIndexName();
		}
		return index;
	}

	private void logStatistics(StopWatch sw) {
		long totalDocuments = deletedDocuments + insertedDocuments;
		long totalTimeInSeconds = sw.stop().totalTime().seconds();
		long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
		logger.info("Indexed {} documents, {} insertions {}, updates, {} deletions, {} documents per second", totalDocuments, insertedDocuments, updatedDocuments, deletedDocuments, totalDocumentsPerSecond);
	}

	@Override
	public void close() {
		keepRunning = false;
	}
}
