package org.elasticsearch.river.arangodb.config;

import static java.util.Collections.unmodifiableSet;
import static net.swisstech.swissarmyknife.util.Sets.newHashSet;
import static org.elasticsearch.common.collect.Maps.newHashMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;

/** config object and holder of shared object */
@Singleton
public class ArangoDbConfig {

	private final String riverIndexName;
	private final String riverName;
	private final BlockingQueue<Map<String, Object>> eventStream;
	private final String arangodbHost;
	private final int arangodbPort;
	private final String arangodbDatabase;
	private final String arangodbCollection;
	private final ExecutableScript arangodbScript;
	private final boolean arangodbOptionsFullSync;
	private final boolean arangodbOptionsDropcollection;
	private final Set<String> arangodbOptionsExcludeFields;
	private final String arangodbCredentialsUsername;
	private final String arangodbCredentialsPassword;
	private final String indexName;
	private final String indexType;
	private final int indexBulkSize;
	private final int indexThrottleSize;
	private final TimeValue indexBulkTimeout;

	@Inject
	public ArangoDbConfig( //
	RiverSettingsWrapper rsw, //
		@RiverIndexName final String riverIndexName, //
		RiverName pRiverName, //
		ScriptService scriptService //
	) {

		this.riverIndexName = riverIndexName;
		riverName = pRiverName.name();

		// arangodb
		arangodbHost = rsw.getString("arangodb.host", "localhost");
		arangodbPort = rsw.getInteger("arangodb.port", 8529);
		arangodbDatabase = rsw.getString("arangodb.db", riverName);
		arangodbCollection = rsw.getString("arangodb.collection", riverName);

		String scriptString = rsw.getString("arangodb.script", null);
		String scriptLang = rsw.getString("arangodb.scriptType", "js");
		arangodbScript = scriptService.executable(scriptLang, scriptString, ScriptType.INLINE, newHashMap());

		// arangodb.options
		arangodbOptionsFullSync = rsw.getBool("arangodb.options.full_sync", false);
		arangodbOptionsDropcollection = rsw.getBool("arangodb.options.drop_collection", true);

		Set<String> excludes = newHashSet("_id", "_key", "_rev");
		excludes.addAll(rsw.getList("arangodb.options.exclude_fields"));
		arangodbOptionsExcludeFields = unmodifiableSet(excludes);

		// arangodb.credentials
		arangodbCredentialsUsername = rsw.getString("arangodb.credentials.username", "");
		arangodbCredentialsPassword = rsw.getString("arangodb.credentials.password", "");

		// index
		indexName = rsw.getString("index.name", riverName);
		indexType = rsw.getString("index.type", riverName);
		indexBulkSize = rsw.getInteger("index.bulk_size", 100);
		indexThrottleSize = rsw.getInteger("index.throttle_size", indexBulkSize * 5);

		String bulkTimeoutString = rsw.getString("index.bulk_timeout", "10ms");
		indexBulkTimeout = TimeValue.parseTimeValue(bulkTimeoutString, null);

		// event stream from producer to consumer
		if (indexThrottleSize == -1) {
			eventStream = new LinkedTransferQueue<Map<String, Object>>();
		}
		else {
			eventStream = new ArrayBlockingQueue<Map<String, Object>>(indexThrottleSize);
		}

	}

	public String getRiverIndexName() {
		return riverIndexName;
	}

	public String getRiverName() {
		return riverName;
	}

	public BlockingQueue<Map<String, Object>> getEventStream() {
		return eventStream;
	}

	public String getArangodbHost() {
		return arangodbHost;
	}

	public int getArangodbPort() {
		return arangodbPort;
	}

	public String getArangodbDatabase() {
		return arangodbDatabase;
	}

	public String getArangodbCollection() {
		return arangodbCollection;
	}

	public ExecutableScript getArangodbScript() {
		return arangodbScript;
	}

	public boolean getArangodbOptionsFullSync() {
		return arangodbOptionsFullSync;
	}

	public boolean isArangodbOptionsDropcollection() {
		return arangodbOptionsDropcollection;
	}

	public Set<String> getArangodbOptionsExcludeFields() {
		return arangodbOptionsExcludeFields;
	}

	public String getArangodbCredentialsUsername() {
		return arangodbCredentialsUsername;
	}

	public String getArangodbCredentialsPassword() {
		return arangodbCredentialsPassword;
	}

	public String getIndexName() {
		return indexName;
	}

	public String getIndexType() {
		return indexType;
	}

	public int getIndexBulkSize() {
		return indexBulkSize;
	}

	public int getIndexThrottleSize() {
		return indexThrottleSize;
	}

	public TimeValue getIndexBulkTimeout() {
		return indexBulkTimeout;
	}
}
