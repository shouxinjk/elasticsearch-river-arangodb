package org.elasticsearch.river.arangodb.config;

import static java.util.Collections.unmodifiableSet;
import static net.swisstech.swissarmyknife.lang.Integers.positive;
import static net.swisstech.swissarmyknife.lang.Longs.positive;
import static net.swisstech.swissarmyknife.lang.Strings.notBlank;
import static net.swisstech.swissarmyknife.net.TCP.validPortNumber;
import static net.swisstech.swissarmyknife.util.Sets.newHashSet;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.arangodb.EventStream;
import org.elasticsearch.script.ScriptService;

/** config object and holder of shared objects */
@Singleton
public class ArangoDbConfig {

	private final String riverIndexName;
	private final String riverName;
	private final EventStream eventStream;
	private final String arangodbHost;
	private final int arangodbPort;
	private final String arangodbDatabase;
	private final String arangodbCollection;
	private final String arangodbScript;
	private final String arangodbScripttype;
	private final long arangodbReaderMinSleep;
	private final long arangodbReaderMaxSleep;
	private final Set<String> arangodbOptionsExcludeFields;
	private final String arangodbCredentialsUsername;
	private final String arangodbCredentialsPassword;
	private final String indexName;
	private final String indexType;
	private final int indexBulkSize;
	private final int indexThrottleSize;
	private final long indexBulkTimeout;

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
		arangodbHost = notBlank(rsw.getString("arangodb.host", "localhost"));
		arangodbPort = validPortNumber(rsw.getInteger("arangodb.port", 8529));
		arangodbDatabase = notBlank(rsw.getString("arangodb.db", riverName));
		arangodbCollection = notBlank(rsw.getString("arangodb.collection", riverName));

		arangodbScript = rsw.getString("arangodb.script", null);
		arangodbScripttype = notBlank(rsw.getString("arangodb.script_type", "js"));

		// arangodb.options
		arangodbReaderMinSleep = positive(rsw.getTimeValue("arangodb.reader_min_sleep", timeValueMillis(100)).millis());
		arangodbReaderMaxSleep = positive(rsw.getTimeValue("arangodb.reader_max_sleep", timeValueMillis(10_000)).millis());

		Set<String> excludes = newHashSet("_id", "_key", "_rev");
		excludes.addAll(rsw.getList("arangodb.options.exclude_fields"));
		arangodbOptionsExcludeFields = unmodifiableSet(excludes);

		// arangodb.credentials
		arangodbCredentialsUsername = rsw.getString("arangodb.credentials.username", "");
		arangodbCredentialsPassword = rsw.getString("arangodb.credentials.password", "");

		// index
		indexName = notBlank(rsw.getString("index.name", riverName));
		indexType = notBlank(rsw.getString("index.type", riverName));
		indexBulkSize = positive(rsw.getInteger("index.bulk_size", 100));
		indexThrottleSize = rsw.getInteger("index.throttle_size", indexBulkSize * 5);
		indexBulkTimeout = positive(rsw.getTimeValue("index.bulk_timeout", timeValueMillis(10)).millis());

		// event stream from producer to consumer
		BlockingQueue<WalEvent> queue = null;
		if (indexThrottleSize == -1) {
			queue = new LinkedTransferQueue<WalEvent>();
		}
		else {
			queue = new ArrayBlockingQueue<WalEvent>(indexThrottleSize);
		}
		eventStream = new EventStream(queue, indexBulkTimeout, indexBulkTimeout);
	}

	public String getRiverIndexName() {
		return riverIndexName;
	}

	public String getRiverName() {
		return riverName;
	}

	public EventStream getEventStream() {
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

	public String getArangodbScript() {
		return arangodbScript;
	}

	public String getArangodbScripttype() {
		return arangodbScripttype;
	}

	public long getArangodbReaderMinSleep() {
		return arangodbReaderMinSleep;
	}

	public long getArangodbReaderMaxSleep() {
		return arangodbReaderMaxSleep;
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

	public long getIndexBulkTimeout() {
		return indexBulkTimeout;
	}
}
