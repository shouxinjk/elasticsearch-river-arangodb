package org.elasticsearch.river.arangodb.es;

import static net.swisstech.swissarmyknife.lang.Strings.isBlank;
import static net.swisstech.swissarmyknife.lang.Strings.notBlank;

import java.util.Map;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;

import com.fasterxml.jackson.databind.JsonNode;

@Singleton
public class ScriptResultProcessor {

	private final String index;
	private final String type;

	@Inject
	public ScriptResultProcessor(ArangoDbConfig config) {
		index = config.getIndexName();
		type = config.getIndexType();
	}

	public void process(Map<String, Object> ctx, EsBulk bulk) {
		String operation = (String) ctx.get("operation");
		if (isBlank(operation) || "SKIP".equals(operation)) {
			return;
		}

		String id = notBlank((String) ctx.get("id"));
		JsonNode doc = (JsonNode) ctx.get("doc");

		if ("DELETE".equals(operation)) {
			DeleteRequest req = new DeleteRequest(index, type, id);
			bulk.add(req);
		}
		else if ("UPDATE".equals(operation)) {
			String json = doc.toString();
			IndexRequest req = new IndexRequest(index, type, id);
			req.source(json);
			bulk.add(req);
		}
		else {
			// TODO something's fishy
		}
	}
}
