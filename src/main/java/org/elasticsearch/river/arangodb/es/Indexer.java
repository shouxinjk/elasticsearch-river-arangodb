package org.elasticsearch.river.arangodb.es;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.script.ExecutableScript;

@Singleton
public class Indexer implements Runnable, Closeable {

	private static final ESLogger logger = ESLoggerFactory.getLogger(Indexer.class.getName());

	private boolean keepRunning = true;

	private int deletedDocuments = 0;
	private int insertedDocuments = 0;
	private int updatedDocuments = 0;

	private final ArangoDbConfig config;
	private final Client client;

	@Inject
	public Indexer(ArangoDbConfig config, Client client) {
		this.config = config;
		this.client = client;
	}

	@Override
	public void run() {
		logger.info("=== river-arangodb indexer running ... ===");

		BlockingQueue<WalEvent> stream = config.getEventStream();

		while (keepRunning) {
			StopWatch sw = new StopWatch().start();

			deletedDocuments = 0;
			insertedDocuments = 0;
			updatedDocuments = 0;

			try {
				String lastTick = null;
				BulkRequestBuilder bulk = client.prepareBulk();

				// 1. Attempt to fill as much of the bulk request as possible
				WalEvent data = stream.take();
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
				logger.debug("river-arangodb indexer interrupted");
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

		logger.debug("updateBulkRequest for id: [{}], operation: [{}]", objectId, operation);
		logger.debug("data: [{}]", data);

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

			logger.debug("Context before script executed: {}", ctx);

			script.setNextVar("ctx", ctx);

			try {
				script.run();
				ctx = (Map<String, Object>) script.unwrap(ctx);
			}
			catch (Exception e) {
				logger.warn("failed to script process {}, ignoring", e, ctx);
			}

			logger.debug("Context after script executed: {}", ctx);

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

			logger.debug("Operation: {} - index: {} - type: {} - routing: {} - parent: {}", operation, index, type, routing, parent);

			if (operation == OpType.INSERT) {
				logger.debug("Insert operation - id: {}", operation, objectId);

				bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));

				insertedDocuments++;
			}
			else if (operation == OpType.UPDATE) {
				logger.debug("Update operation - id: {}", objectId);

				bulk.add(new DeleteRequest(index, type, objectId).routing(routing).parent(parent));
				bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));

				updatedDocuments++;
			}
			else if (operation == OpType.DELETE) {
				logger.debug("Delete operation - id: {}, data [{}]", objectId, data);

				if (REPLOG_ENTRY_UNDEFINED.equals(objectId) && data.get(NAME_FIELD).equals(config.getArangodbCollection())) {
					if (config.getArangodbDropcollection()) {
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
			XContentBuilder json = jsonBuilder() //
				.startObject() //
				.startObject(RIVER_TYPE) //
				.field(LAST_TICK_FIELD, tick) //
				.endObject() //
				.endObject();

			IndexRequest req = indexRequest(riverIndexName) //
				.type(riverName.getName()) //
				.id(namespace) //
				.source(json);

			bulk.add(req);
		}
		catch (IOException e) {
			logger.error("error updating last Tick for collection {}", namespace);
		}
	}

	@Override
	public void close() {
		keepRunning = false;
	}
}
