package org.elasticsearch.river.arangodb;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.collect.Maps.newHashMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;

@Singleton
public class ArangoDbConfig {

	private final String arangodbHost;
	private final int arangodbPort;
	private final String arangodbDatabase;
	private final String arangodbCollection;
	private final ExecutableScript arangodbScript;
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
	public ArangoDbConfig(RiverSettingsWrapper rsw, ScriptService scriptService, RiverName riverName) {

		// arangodb
		arangodbHost = rsw.getString("arangodb.host", "localhost");
		arangodbPort = rsw.getInt("arangodb.port", 8529);
		arangodbDatabase = rsw.getString("arangodb.db", riverName.name());
		arangodbCollection = rsw.getString("arangodb.collection", riverName.name());

		String scriptString = rsw.getString("arangodb.script", null);
		String scriptLang = rsw.getString("arangodb.scriptType", "js");
		ScriptType scriptType = ScriptType.INLINE;
		arangodbScript = scriptService.executable(scriptLang, scriptString, scriptType, newHashMap());

		// arangodb.options
		arangodbOptionsDropcollection = rsw.getBool("arangodb.options.drop_collection", true);

		Set<String> excludes = new HashSet<String>();
		excludes.add("_id");
		excludes.add("_key");
		excludes.add("_rev");
		excludes.addAll(rsw.getList("arangodb.options.exclude_fields", new ArrayList<String>()));
		arangodbOptionsExcludeFields = unmodifiableSet(excludes);

		// arangodb.credentials
		arangodbCredentialsUsername = rsw.getString("arangodb.credentials.username", "");
		arangodbCredentialsPassword = rsw.getString("arangodb.credentials.password", "");

		// index
		indexName = rsw.getString("index.name", riverName.name());
		indexType = rsw.getString("index.type", riverName.name());
		indexBulkSize = rsw.getInt("index.bulk_size", 100);
		indexThrottleSize = rsw.getInt("index.throttle_size", indexBulkSize * 5);

		String bulkTimeoutString = rsw.getString("index.bulk_timeout", "10ms");
		indexBulkTimeout = TimeValue.parseTimeValue(bulkTimeoutString, null);
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
