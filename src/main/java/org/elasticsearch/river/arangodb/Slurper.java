package org.elasticsearch.river.arangodb;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.river.arangodb.ArangoConstants.HTTP_HEADER_CHECKMORE;
import static org.elasticsearch.river.arangodb.ArangoConstants.HTTP_HEADER_LASTINCLUDED;
import static org.elasticsearch.river.arangodb.ArangoConstants.HTTP_PROTOCOL;
import static org.elasticsearch.river.arangodb.ArangoConstants.NAME_FIELD;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_ENTRY_UNDEFINED;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_KEY;
import static org.elasticsearch.river.arangodb.ArangoConstants.REPLOG_FIELD_TICK;
import static org.elasticsearch.river.arangodb.ArangoConstants.STREAM_FIELD_OPERATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.json.JSONException;

public class Slurper implements Runnable {

	private static final ESLogger logger = ESLoggerFactory.getLogger(Slurper.class.getName());

	private List<ReplogEntity> replogCursorResultSet;
	private String currentTick;
	private CloseableHttpClient arangoHttpClient;

	private final Set<String> excludeFields;
	private final String arangoCollection;
	private final String arangoDb;
	private final String arangoAdminUser;
	private final String arangoAdminPassword;
	private final String arangoHost;
	private final int arangoPort;
	private final BlockingQueue<Map<String, Object>> stream;
	private final ArangoDBRiver river;

	public Slurper( //
	String lastProcessedTick, //
		Set<String> excludeFields, //
		String arangoCollection, //
		String arangoDb, //
		String arangoAdminUser, //
		String arangoAdminPassword, //
		String arangoHost, //
		int arangoPort, //
		BlockingQueue<Map<String, Object>> stream, //
		ArangoDBRiver river //
	) {
		currentTick = lastProcessedTick;
		this.excludeFields = unmodifiableSet(excludeFields);
		this.arangoCollection = arangoCollection;
		this.arangoDb = arangoDb;
		this.arangoAdminUser = arangoAdminUser;
		this.arangoAdminPassword = arangoAdminPassword;
		this.arangoHost = arangoHost;
		this.arangoPort = arangoPort;
		this.stream = stream;
		this.river = river;
	}

	@Override
	public void run() {
		logger.info("=== river-arangodb slurper running ... ===");

		while (river.isActive()) {
			try {
				replogCursorResultSet = processCollection(currentTick);
				ReplogEntity last_item = null;

				for (ReplogEntity item : replogCursorResultSet) {
					if (logger.isDebugEnabled()) {
						logger.debug("slurper: processReplogEntry [{}]", item);
					}

					processReplogEntry(item);
					last_item = item;
				}

				if (last_item != null) {
					currentTick = last_item.getTick();

					if (logger.isDebugEnabled()) {
						logger.debug("slurper: last_item currentTick [{}]", currentTick);
					}
				}

				Thread.sleep(2000);
			}
			catch (ArangoException e) {
				logger.error("slurper: ArangoDB exception ", e);

				// refactoring: call back into river. before the refactoring it just assigned 'active = false' which means everything will stop
				river.close();
			}
			catch (InterruptedException e) {
				if (logger.isDebugEnabled()) {
					logger.debug("river-arangodb slurper interrupted");
				}
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

		if (logger.isDebugEnabled()) {
			logger.debug("replog entry - collection [{}], operation [{}]", arangoCollection, operation);
			logger.debug("replog processing item [{}]", entry);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("processReplogEntry - entry.getKey() [{}]", entry.getKey());
			logger.trace("processReplogEntry - entry.getRev() [{}]", entry.getRev());
			logger.trace("processReplogEntry - entry.getOperation() [{}]", entry.getOperation());
			logger.trace("processReplogEntry - entry.getTick() [{}]", entry.getTick());
			logger.trace("processReplogEntry - entry.getData() [{}]", entry.getData());
		}

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
			for (String excludeField : excludeFields) {
				data.remove(excludeField);
			}
		}

		addToStream(documentHandle, operation, replogTick, data);
	}

	private void addToStream(final String documentHandle, final OpType operation, final String tick, final Map<String, Object> data) throws InterruptedException {
		if (logger.isDebugEnabled()) {
			logger.debug("addToStream - operation [{}], currentTick [{}], data [{}]", operation, tick, data);
		}

		if (documentHandle.equals(REPLOG_ENTRY_UNDEFINED)) {
			data.put(NAME_FIELD, arangoCollection);
		}

		data.put(REPLOG_FIELD_KEY, documentHandle);
		data.put(REPLOG_FIELD_TICK, tick);
		data.put(STREAM_FIELD_OPERATION, operation);

		stream.put(data);
	}

	private List<ReplogEntity> getNextArangoDBReplogs(String currentTick) throws ArangoException, JSONException, IOException {
		List<ReplogEntity> replogs = new ArrayList<ReplogEntity>();

		CloseableHttpClient httpClient = getArangoHttpClient();
		String uri = getReplogUri();

		if (logger.isDebugEnabled()) {
			logger.debug("http uri = {}", uri + currentTick);
		}

		boolean checkMore = true;

		while (checkMore) {
			HttpGet httpGet = new HttpGet(uri + currentTick);

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

	private String getReplogUri() {
		String uri = HTTP_PROTOCOL + "://";
		uri += arangoHost + ":" + arangoPort;
		uri += "/_db/" + arangoDb + "/_api/replication/dump?collection=";
		uri += arangoCollection + "&from=";
		return uri;
	}

	private CloseableHttpClient getArangoHttpClient() {
		if (arangoHttpClient == null) {
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
			AuthScope authScope = new AuthScope(arangoHost, arangoPort);
			UsernamePasswordCredentials unpwCreds = new UsernamePasswordCredentials(arangoAdminUser, arangoAdminPassword);
			credsProvider.setCredentials(authScope, unpwCreds);
			arangoHttpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
			logger.info("created ArangoDB http client");
		}

		return arangoHttpClient;
	}

	public void shutdown() {
		try {
			arangoHttpClient.close();
		}
		catch (IOException e) {
			logger.error("River method close threw an IO exception", e);
		}

		try {
			arangoHttpClient.close();
		}
		catch (Exception e) {
			logger.error("Http client method close threw an exception", e);
		}
	}
}
