package org.elasticsearch.river.arangodb;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.codehaus.jackson.map.ObjectMapper;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
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

import org.json.JSONObject;
import org.json.JSONException;

public class ArangoDBRiver extends AbstractRiverComponent implements River {
	public final static String RIVER_TYPE = "arangodb";
	public final static String DB_FIELD = "db";
	public final static String HOST_FIELD = "host";
	public final static String PORT_FIELD = "port";
	public final static String OPTIONS_FIELD = "options";
	public final static String DROP_COLLECTION_FIELD = "drop_collection";
	public final static String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	public final static String CREDENTIALS_FIELD = "credentials";
	public final static String USER_FIELD = "username";
	public final static String PASSWORD_FIELD = "password";
	public final static String SCRIPT_FIELD = "script";
	public final static String SCRIPT_TYPE_FIELD = "scriptType";
	public final static String COLLECTION_FIELD = "collection";
	public final static String INDEX_OBJECT = "index";
	public final static String NAME_FIELD = "name";
	public final static String TYPE_FIELD = "type";
	public final static String DEFAULT_DB_HOST = "localhost";
	public final static int    DEFAULT_DB_PORT = 8529;
	public final static String THROTTLE_SIZE_FIELD = "throttle_size";
	public final static String BULK_SIZE_FIELD = "bulk_size";
	public final static String BULK_TIMEOUT_FIELD = "bulk_timeout";
	public final static String LAST_TICK_FIELD = "_last_tick";
	public final static String REPLOG_ENTRY_UNDEFINED = "undefined";
	public final static String REPLOG_FIELD_KEY = "key";
	public final static String REPLOG_FIELD_TICK = "tick";
	public final static List   REPLOG_SLURPED_TYPES = asList(2000, 2300, 2302);
	public final static String STREAM_FIELD_OPERATION = "op";

	public final static String HTTP_PROTOCOL = "http";
	public final static String HTTP_HEADER_CHECKMORE = "x-arango-replication-checkmore";
	public final static String HTTP_HEADER_LASTINCLUDED = "x-arango-replication-lastincluded";

	protected final Client client;
	protected final ObjectMapper mapper;

	protected final String riverIndexName;

	protected final List<ServerAddress> arangoServers = new ArrayList<ServerAddress>();
	protected final String arangoDb;
	protected final String arangoCollection;
	protected final String arangoAdminUser;
	protected final String arangoAdminPassword;

	protected final String indexName;
	protected final String typeName;
	protected final int bulkSize;
	protected final TimeValue bulkTimeout;
	protected final int throttleSize;
	protected final boolean dropCollection;
	
	protected final ArrayList<String> basicExcludeFields = new ArrayList<String>(asList("_id", "_key", "_rev"));
	protected Set<String> excludeFields = new HashSet<String>();

	private final ExecutableScript script;

	protected volatile List<Thread> slurperThreads = new ArrayList<Thread>();
	protected volatile Thread indexerThread;
	protected volatile boolean active = true;

	private final BlockingQueue<Map<String, Object>> stream;
	
	private String arangoHost;
	private int arangoPort;
	private CloseableHttpClient arangoHttpClient;
	
	private String currentTick;

	@SuppressWarnings("unchecked")
	@Inject
	public ArangoDBRiver(final RiverName riverName, final RiverSettings settings,
			@RiverIndexName final String riverIndexName, final Client client, final ScriptService scriptService) throws ArangoException {
		super(riverName, settings);
		
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Prefix: [{}] - name: [{}]", this.logger.getPrefix(), this.logger.getName());
			this.logger.debug("River settings: [{}]", settings.settings());
		}
		
		this.riverIndexName = riverIndexName;
		this.client = client;
		this.mapper = new ObjectMapper();
		
		for (String field : basicExcludeFields) {
			this.excludeFields.add(field);
		}

		if (settings.settings().containsKey(RIVER_TYPE)) {
			Map<String, Object> arangoSettings = (Map<String, Object>) settings.settings().get(RIVER_TYPE);
			
			this.arangoHost = XContentMapValues.nodeStringValue(arangoSettings.get(HOST_FIELD), DEFAULT_DB_HOST);
			this.arangoPort = XContentMapValues.nodeIntegerValue(arangoSettings.get(PORT_FIELD), DEFAULT_DB_PORT);
				
			this.arangoServers.add(new ServerAddress(this.arangoHost, this.arangoPort));

			// ArangoDB options
			if (arangoSettings.containsKey(OPTIONS_FIELD)) {
				Map<String, Object> arangoOptionsSettings = (Map<String, Object>) arangoSettings.get(OPTIONS_FIELD);
				
				this.dropCollection = XContentMapValues.nodeBooleanValue(arangoOptionsSettings.get(DROP_COLLECTION_FIELD), true);

				if (arangoOptionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) {
					Object excludeFieldsSettings = arangoOptionsSettings.get(EXCLUDE_FIELDS_FIELD);
					
					this.logger.info("excludeFieldsSettings: " + excludeFieldsSettings);

					if (XContentMapValues.isArray(excludeFieldsSettings)) {
						ArrayList<String> fields = (ArrayList<String>) excludeFieldsSettings;
						
						for (String field : fields) {
							this.logger.info("Field: " + field);
							this.excludeFields.add(field);
						}
					}
				}
			} else {
				this.dropCollection = true;
			}

			// Credentials
			if (arangoSettings.containsKey(CREDENTIALS_FIELD)) {
				Map<String, Object> credentials = (Map<String, Object>) arangoSettings.get(CREDENTIALS_FIELD);
				
				this.arangoAdminUser = XContentMapValues.nodeStringValue(credentials.get(USER_FIELD), null);
				this.arangoAdminPassword = XContentMapValues.nodeStringValue(credentials.get(PASSWORD_FIELD), null);

			} else {
				this.arangoAdminUser = "";
				this.arangoAdminPassword = "";
			}

			this.arangoDb = XContentMapValues.nodeStringValue(arangoSettings.get(DB_FIELD), riverName.name());
			this.arangoCollection = XContentMapValues.nodeStringValue(arangoSettings.get(COLLECTION_FIELD), riverName.name());

			if (arangoSettings.containsKey(SCRIPT_FIELD)) {
				String scriptType = "js";
				
				if (arangoSettings.containsKey(SCRIPT_TYPE_FIELD)) {
					scriptType = arangoSettings.get(SCRIPT_TYPE_FIELD).toString();
				}

				this.script = scriptService.executable(scriptType, arangoSettings.get(SCRIPT_FIELD).toString(), Maps.newHashMap());
			} else {
				this.script = null;
			}
		} else {
			this.arangoHost = DEFAULT_DB_HOST;
			this.arangoPort = DEFAULT_DB_PORT;
			
			this.arangoServers.add(new ServerAddress(this.arangoHost, this.arangoPort));
			
			this.arangoDb = riverName.name();
			this.arangoCollection = riverName.name();
			this.arangoAdminUser = "";
			this.arangoAdminPassword = "";
			this.script = null;
			this.dropCollection = true;
		}

		if (settings.settings().containsKey(INDEX_OBJECT)) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get(INDEX_OBJECT);
			
			this.indexName = XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), arangoDb);
			this.typeName = XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), arangoDb);
			this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE_FIELD), 100);
			
			if (indexSettings.containsKey(BULK_TIMEOUT_FIELD)) {
				this.bulkTimeout = TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(indexSettings.get(BULK_TIMEOUT_FIELD), "10ms"),
						TimeValue.timeValueMillis(10));
			} else {
				this.bulkTimeout = TimeValue.timeValueMillis(10);
			}
			
			throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get(THROTTLE_SIZE_FIELD), bulkSize * 5);
		
		} else {
			this.indexName = arangoDb;
			this.typeName = arangoDb;
			this.bulkSize = 100;
			this.bulkTimeout = TimeValue.timeValueMillis(10);
			this.throttleSize = bulkSize * 5;
		}
		
		if (throttleSize == -1) {
			this.stream = new LinkedTransferQueue<Map<String, Object>>();
		} else {
			this.stream = new ArrayBlockingQueue<Map<String, Object>>(throttleSize);
		}
	}

	@Override
	public void start() {
		for (ServerAddress server : this.arangoServers) {
			this.logger.info("using arangodb server(s): host [{}], port [{}]", server.getHost(), server.getPort());
		}
		
		this.logger.info("starting arangodb stream. options: throttlesize [{}], db [{}], collection [{}], script [{}], indexing to [{}]/[{}]",
				this.throttleSize, 
				this.arangoDb, 
				this.arangoCollection, 
				this.script, 
				this.indexName, 
				this.typeName);
		
		try {
			this.client.admin().indices().prepareCreate(indexName).execute().actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// ok
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ..
			} else {
				this.logger.warn("failed to create index [{}], disabling river...", e, indexName);
				return;
			}
		}
		
		String lastProcessedTick = this.fetchLastTick(this.arangoCollection);
		
		Thread slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_slurper").newThread(new Slurper(arangoServers, lastProcessedTick));
			
		this.slurperThreads.add(slurperThread);

		for (Thread thread : slurperThreads) {
			this.logger.info("starting arangodb slurper [{}]", thread);
			thread.start();
		}

		this.indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "arangodb_river_indexer").newThread(new Indexer());
		this.indexerThread.start();
		this.logger.info("starting arangodb indexer");
	}

	@Override
	public void close() {
		if (this.active) {
			this.logger.info("closing arangodb stream river");
			this.active = false;
			
			for (Thread thread : this.slurperThreads) {
				thread.interrupt();
				this.logger.info("stopping arangodb slurper [{}]", thread);
			}
			
			this.indexerThread.interrupt();
			this.logger.info("stopping arangodb indexer");
			
			try {
				this.arangoHttpClient.close();
			} catch (IOException iEx) {
				this.logger.error("River method close threw an IO exception", iEx);
			}
			
			try {
				this.arangoHttpClient.close();
			} catch (Exception ex) {
				this.logger.error("Http client method close threw an exception", ex);
			}
		}
	}
	
	private ServerAddress getActiveMaster() {
		return this.arangoServers.get(0);
	}

	private CloseableHttpClient getArangoHttpClient() {
		if (this.arangoHttpClient == null) {
			ServerAddress activeServerAddress = this.getActiveMaster();
			
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();
	        
	        credsProvider.setCredentials(
	        		new AuthScope(activeServerAddress.getHost(), activeServerAddress.getPort()),
	                new UsernamePasswordCredentials(this.arangoAdminUser, this.arangoAdminPassword));
	        
	        this.arangoHttpClient = HttpClients.custom()
	                .setDefaultCredentialsProvider(credsProvider).build();
			
	        this.logger.info("created ArangoDB http client");
		}
		
		return this.arangoHttpClient;
	}
	
	private String getReplogUri() {
		ServerAddress activeServerAddress = this.getActiveMaster();
        
		String uri = HTTP_PROTOCOL + "://";
		uri += activeServerAddress.getHost() + ":" + activeServerAddress.getPort();
		uri += "/_db/" + this.arangoDb + "/_api/replication/dump?collection=";
		uri += this.arangoCollection + "&from=";
        
		return uri;
	}
	
	private String fetchLastTick(final String namespace) {
		String lastTick = null;
		
		this.logger.info("fetching last tick for collection {}", namespace);
		
		GetResponse stateResponse = client
				.prepareGet(riverIndexName, riverName.getName(), namespace)
				.execute().actionGet();
		
		if (stateResponse.isExists()) {
			Map<String, Object> indexState = (Map<String, Object>) stateResponse.getSourceAsMap().get(RIVER_TYPE);
			
			if (indexState != null) {
				try {
					lastTick = indexState.get(LAST_TICK_FIELD).toString();
					
					this.logger.info("found last tick for collection {}: {}", namespace, lastTick);
					
				} catch (Exception ex) {
					this.logger.error("error fetching last tick for collection {}: {}", namespace, ex);
				}
			} else {
				this.logger.info("fetching last tick: indexState is null");
			}
		}
		
		return lastTick;
	}


	private boolean isDeleteOperation(String op) {
		return "DELETE".equals(op);
	}
	
	private boolean isInsertOperation(String op) {
		return "INSERT".equals(op);
	}
	
	private boolean isUpdateOperation(String op) {
		return "UPDATE".equals(op);
	}

	private List<ReplogEntity> getNextArangoDBReplogs(String currentTick) throws ArangoException, JSONException, IOException {
		List<ReplogEntity> replogs = new ArrayList<ReplogEntity>();

		CloseableHttpClient httpClient = this.getArangoHttpClient();
		String uri = this.getReplogUri();

		if (logger.isDebugEnabled()) {
			this.logger.debug("http uri = {}", uri + currentTick);
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

				} finally {
					response.close();
				}
	
			} else if (status == 404) {
				checkMore = false;
			
			} else {
				throw new ArangoException("unexpected http response status: " + status);
			}
		}

		return replogs;
	}

	private class ArangoException extends Exception {
		public ArangoException() {
			super();
		}
        
		public ArangoException(String message) {
			super(message);
		}

		public ArangoException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private class ServerAddress {
		private String host;
		private int port;
		
		public ServerAddress(String host, int port) {
			this.host = host;
			this.port = port;
		}
		
		public String getHost() {
			return this.host;
		}
		
		public int getPort() {
			return this.port;
		}
	}
	
	private class ReplogEntity extends JSONObject {
		public ReplogEntity(String str) throws JSONException {
			super(str);
		}
		
		private String getTick() {
			try {
				return (String) this.get("tick");
			} catch (JSONException e) {
				logger.error("error in ReplogEntity method 'getTick'");
				return null;
			}
		}
		
		private int getType() {
			try {
				return (Integer) this.get("type");
			} catch (JSONException e) {
				logger.error("error in ReplogEntity method 'getType'");
				return 0;
			}
		}
		
		private String getKey() {
			try {
				return (String) this.get("key");
			} catch (JSONException e) {
				logger.error("error in ReplogEntity method 'getKey'");
				return null;
			}
		}
		
		private String getRev() {
			try {
				return (String) this.get("rev");
			} catch (JSONException e) {
				logger.error("error in ReplogEntity method 'getRev'");
				return null;
			}
		}
		
		private Map<String, Object> getData() {
			try {
				if (this.has("data")) {
					return mapper.readValue(this.get("data").toString(), HashMap.class);
				} else {
					return null;
				}
			} catch (IOException e) {
				logger.error("Found IOException in ReplogEntity method 'getData'.");
				return null;
			} catch (JSONException e) {
				logger.error("Found JSONException in ReplogEntity method 'getData'.");
				return null;
			}
		}
		
		private String getOperation() {
			if (this.getType() == 2302) {
				return "DELETE";
			} else {
				return "UPDATE";
			}
		}
	}

	private class Slurper implements Runnable {
		private List<ReplogEntity> replogCursorResultSet;
		private String currentTick;
		private final List<ServerAddress> arangoServers = null;

		public Slurper(List<ServerAddress> arangoServers, String lastProcessedTick) {
			arangoServers = arangoServers;
			currentTick = lastProcessedTick;
		}

		@Override
		public void run() {
			logger.info("=== river-arangodb slurper running ... ===");

			while (active) {
				try {
					replogCursorResultSet = this.processCollection(currentTick);
					ReplogEntity last_item = null;
					
					for (ReplogEntity item : replogCursorResultSet) {
						if (logger.isDebugEnabled()) {
							logger.debug("slurper: processReplogEntry [{}]", item);
						}
						
						this.processReplogEntry(item);
						last_item = item;
					}
					
					if (last_item != null) {
						currentTick = last_item.getTick();
						
						if (logger.isDebugEnabled()) {
							logger.debug("slurper: last_item currentTick [{}]", currentTick);
						}
					}
					
					Thread.sleep(2000);
					
				} catch (ArangoException aEx) {
					logger.error("slurper: ArangoDB exception ", aEx);
					active = false;
					
				} catch (InterruptedException e) {
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
			} catch (ArangoException aEx) {
				logger.error("ArangoDB getNextArangoDBReplogs threw an Arango exception", aEx);
			} catch (JSONException jEx) {
				logger.error("ArangoDB getNextArangoDBReplogs threw a JSON exception", jEx);
			} catch (IOException iEx) {
				logger.error("ArangoDB getNextArangoDBReplogs threw an IO exception", iEx);
			}
			
			return res;
		}
		
		private boolean check_type(final ReplogEntity entry) {
			if (REPLOG_SLURPED_TYPES.contains(entry.getType())) {
				return true;
			} else {
				logger.error("Sorry: Don't know how to handle replog type {}.", entry.getType());
				return false;
			}
		}

		@SuppressWarnings("unchecked")
		private void processReplogEntry(final ReplogEntity entry) throws ArangoException, InterruptedException {
			String documentHandle = entry.getKey();
			String operation = entry.getOperation();
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

			if (isInsertOperation(operation)) {
				data = entry.getData();
			} else if (isUpdateOperation(operation)) {
				data = (Map<String, Object>) entry.getData();
			}
				
			if (data == null) {
				data = new HashMap<String, Object>();
			} else {
				for (String excludeField : excludeFields) {
					data.remove(excludeField);
				}
			}
				
			addToStream(documentHandle, operation, replogTick, data);
		}

		private void addToStream(final String documentHandle, final String operation, final String tick, final Map<String, Object> data) throws InterruptedException {
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
					lastTick = this.updateBulkRequest(bulk, data);
					
					while ((data = stream.poll(bulkTimeout.millis(), MILLISECONDS)) != null) {
						lastTick = this.updateBulkRequest(bulk, data);
						
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

		@SuppressWarnings({ "unchecked" })
		private String updateBulkRequest(final BulkRequestBuilder bulk, Map<String, Object> data) {
			String replogTick = (String) data.get(REPLOG_FIELD_TICK);
			String operation = (String) data.get(STREAM_FIELD_OPERATION);
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
					
					if (this.logger.isDebugEnabled()) {
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
						operation = ctx.get("operation").toString();
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
				
				if (isInsertOperation(operation)) {
					if (logger.isDebugEnabled()) {
						logger.debug("Insert operation - id: {}", operation, objectId);
					}
					
					bulk.add(indexRequest(index).type(type).id(objectId)
							.source(build(data, objectId)).routing(routing)
							.parent(parent));
					
					insertedDocuments++;
				} else if (isUpdateOperation(operation)) {
					if (logger.isDebugEnabled()) {
						logger.debug("Update operation - id: {}", objectId);
					}
					
					bulk.add(new DeleteRequest(index, type, objectId).routing(routing).parent(parent));
					bulk.add(indexRequest(index).type(type).id(objectId).source(build(data, objectId)).routing(routing).parent(parent));
					
					updatedDocuments++;
				} else if (isDeleteOperation(operation)) {
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
				this.logger.warn("failed to parse {}", e, data);
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
