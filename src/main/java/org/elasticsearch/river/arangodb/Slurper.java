package org.elasticsearch.river.arangodb;

import static org.elasticsearch.river.arangodb.ArangoConstants.LAST_TICK_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.NAME_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_ENTRY_UNDEFINED;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_KEY;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_TICK;
import static org.elasticsearch.river.arangodb.ArangoConstants.RIVER_TYPE;
import static org.elasticsearch.river.arangodb.ArangoConstants.STREAM_FIELD_OPERATION;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.swisstech.swissarmyknife.io.Closeables;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.json.JSONException;

@Singleton
public class Slurper implements Runnable, Closeable {

	private static final ESLogger logger = ESLoggerFactory.getLogger(Slurper.class.getName());

	private static final String HTTP_HEADER_CHECKMORE = "x-arango-replication-checkmore";
	private static final String HTTP_HEADER_LASTINCLUDED = "x-arango-replication-lastincluded";

	private volatile boolean keepRunning = true;

	private String currentTick;
	private CloseableHttpClient arangoHttpClient;

	private final ArangoDbConfig config;
	private final Client client;
	private final String riverIndexName;
	private final RiverName riverName;
	private CloseableHttpClient httpClient;
	private final BlockingQueue<Map<String, Object>> stream;
	private final String replogUriTemplate;

	@Inject
	public Slurper( //
	ArangoDbConfig config, //
		Client client, //
		@RiverIndexName final String riverIndexName, //
		RiverName riverName, //
		@Named("arangodb_river_httpclient") CloseableHttpClient httpClient, //
		@Named("arangodb_river_eventstream") BlockingQueue<Map<String, Object>> stream //
	) {
		this.config = config;
		this.client = client;
		this.riverIndexName = riverIndexName;
		this.riverName = riverName;
		this.httpClient = httpClient;
		this.stream = stream;

		replogUriTemplate = new StringBuilder().append("http://") //
			.append(config.getArangodbHost()) //
			.append(":") //
			.append(config.getArangodbPort()) //
			.append("/_db/") //
			.append(config.getArangodbDatabase()) //
			.append("/_api/replication/dump?collection=") //
			.append(config.getArangodbCollection()) //
			.append("&from=") //
			.toString();
	}

	@Override
	public void run() {
		logger.info("=== river-arangodb slurper running ... ===");

		currentTick = fetchLastTick(config.getArangodbCollection());

		while (keepRunning) {
			try {
				List<ReplogEntity> replogCursorResultSet = processCollection(currentTick);
				ReplogEntity last_item = null;

				for (ReplogEntity item : replogCursorResultSet) {
					logger.debug("slurper: processReplogEntry [{}]", item);

					processReplogEntry(item);
					last_item = item;
				}

				if (last_item != null) {
					currentTick = last_item.getTick();

					logger.debug("slurper: last_item currentTick [{}]", currentTick);
				}

				Thread.sleep(2000);
			}
			catch (ArangoException e) {
				logger.error("slurper: ArangoDB exception ", e);
			}
			catch (InterruptedException e) {
				logger.debug("river-arangodb slurper interrupted");
				logger.error("slurper: InterruptedException ", e);
				Thread.currentThread().interrupt();
			}
		}
	}

	private List<ReplogEntity> processCollection(String currentTick) {
		List<ReplogEntity> res = null;

		try {
			res = getNextArangoDBReplogs(currentTick);
		}
		catch (ArangoException e) {
			logger.error("ArangoDB getNextArangoDBReplogs threw an Arango exception", e);
		}
		catch (JSONException e) {
			logger.error("ArangoDB getNextArangoDBReplogs threw a JSON exception", e);
		}
		catch (IOException e) {
			logger.error("ArangoDB getNextArangoDBReplogs threw an IO exception", e);
		}

		return res;
	}

	private void processReplogEntry(final ReplogEntity entry) throws ArangoException, InterruptedException {
		String documentHandle = entry.getKey();
		OpType operation = entry.getOperation();
		String replogTick = entry.getTick();

		logger.debug("replog entry - collection [{}], operation [{}]", config.getArangodbCollection(), operation);
		logger.debug("replog processing item [{}]", entry);
		logger.trace("processReplogEntry - entry.getKey() [{}]", entry.getKey());
		logger.trace("processReplogEntry - entry.getRev() [{}]", entry.getRev());
		logger.trace("processReplogEntry - entry.getOperation() [{}]", entry.getOperation());
		logger.trace("processReplogEntry - entry.getTick() [{}]", entry.getTick());
		logger.trace("processReplogEntry - entry.getData() [{}]", entry.getData());

		Map<String, Object> data = null;

		if (OpType.INSERT == operation) {
			data = entry.getData();
		}
		else if (OpType.UPDATE == operation) {
			data = entry.getData();
		}

		if (data == null) {
			data = new HashMap<String, Object>();
		}
		else {
			for (String excludeField : config.getArangodbOptionsExcludeFields()) {
				data.remove(excludeField);
			}
		}

		addToStream(documentHandle, operation, replogTick, data);
	}

	private void addToStream(final String documentHandle, final OpType operation, final String tick, final Map<String, Object> data) throws InterruptedException {
		logger.debug("addToStream - operation [{}], currentTick [{}], data [{}]", operation, tick, data);

		if (documentHandle.equals(REPLOG_ENTRY_UNDEFINED)) {
			data.put(NAME_FIELD, config.getArangodbCollection());
		}

		data.put(REPLOG_FIELD_KEY, documentHandle);
		data.put(REPLOG_FIELD_TICK, tick);
		data.put(STREAM_FIELD_OPERATION, operation);

		stream.put(data);
	}

	private List<ReplogEntity> getNextArangoDBReplogs(String currentTick) throws ArangoException, JSONException, IOException {
		List<ReplogEntity> replogs = new ArrayList<ReplogEntity>();
		String uri = replogUriTemplate + currentTick;

		logger.debug("http uri = {}", uri);

		boolean checkMore = true;

		while (checkMore) {
			HttpGet httpGet = new HttpGet(uri);

			CloseableHttpResponse response = httpClient.execute(httpGet);
			int status = response.getStatusLine().getStatusCode();

			if (status >= 200 && status < 300) {
				try {
					HttpEntity entity = response.getEntity();

					if (entity != null) {
						for (String str : EntityUtils.toString(entity).split("\\n")) {
							replogs.add(new ReplogEntity(str));
						}
						currentTick = response.getFirstHeader(HTTP_HEADER_LASTINCLUDED).getValue();
					}

					EntityUtils.consumeQuietly(entity);
					checkMore = Boolean.valueOf(response.getFirstHeader(HTTP_HEADER_CHECKMORE).getValue());
				}
				finally {
					response.close();
				}
			}
			else if (status == 404) {
				checkMore = false;
			}
			else {
				throw new ArangoException("unexpected http response status: " + status);
			}
		}

		return replogs;
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

	@Override
	public void close() {
		keepRunning = false;
		Closeables.close(arangoHttpClient);
	}
}
