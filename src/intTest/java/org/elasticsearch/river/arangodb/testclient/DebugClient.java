package org.elasticsearch.river.arangodb.testclient;

import static net.swisstech.swissarmyknife.lang.Objects.notNull;
import static org.elasticsearch.river.arangodb.util.JacksonUtil.MAPPER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import net.swisstech.arangodb.MgmtClient;
import net.swisstech.arangodb.MgmtClient.CreateCollectionResponse;
import net.swisstech.arangodb.MgmtClient.DeleteCollectionResponse;
import net.swisstech.log.Logger;
import net.swisstech.log.LoggerFactory;

import org.elasticsearch.plugin.river.arangodb.ArangoDbRiverPlugin;
import org.elasticsearch.river.arangodb.testclient.config.Meta;
import org.elasticsearch.river.arangodb.testclient.query.SearchResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public class DebugClient {

	private static final Logger LOG = LoggerFactory.getLogger(DebugClient.class);

	private static final OkHttpClient OKHTTP = new OkHttpClient();
	private static final MediaType MEDIATYPE_JSON = notNull(MediaType.parse("application/json; charset=utf-8"));

	public static void startRiver(String arangoDb, String arangoCollection, String arangoHost, int arangoPort, String riverName, String esHost, int esPort, String indexName, String indexType) throws IOException {
		Meta meta = Meta.create(arangoDb, arangoCollection, arangoHost, arangoPort, indexName, indexType);
		String json = MAPPER.writeValueAsString(meta);
		LOG.debug("Sending Request to ES: %s", json);

		RequestBody body = RequestBody.create(MEDIATYPE_JSON, json);
		Request req = new Request.Builder().url("http://" + esHost + ":" + esPort + "/_river/" + riverName + "/_meta").put(body).build();
		Response rsp = OKHTTP.newCall(req).execute();

		LOG.debug("Response from ES: %s", rsp.body().string());
		assertEquals(rsp.code(), 201);
	}

	public static void deleteCollection(String host, int port, String db, String collectionName) throws IOException {
		MgmtClient mc = new MgmtClient("http://" + host + ":" + port + "/_db/" + db);
		DeleteCollectionResponse dcrsp = mc.deleteCollection(collectionName);
		int code = dcrsp.getCode();
		if (code != 200) {
			fail("deleting collection in arangodb failed: " + MAPPER.writeValueAsString(dcrsp));
		}
	}

	public static void createCollection(String host, int port, String db, String collectionName) throws IOException {
		MgmtClient mc = new MgmtClient("http://" + host + ":" + port + "/_db/" + db);
		CreateCollectionResponse ccrsp = mc.createCollection(collectionName);
		int code = ccrsp.getCode();
		if (code != 200 && code != 409) {
			fail("creating collection in arangodb failed: " + MAPPER.writeValueAsString(ccrsp));
		}
	}

	public static void checkPluginInstalled(String esHost, int esPort) throws IOException {
		Request req = new Request.Builder().url("http://" + esHost + ":" + esPort + "/_nodes").get().build();
		Response rsp = OKHTTP.newCall(req).execute();
		InputStream in = rsp.body().byteStream();
		JsonNode root = MAPPER.readValue(in, JsonNode.class);

		JsonNode nodes = root.get("nodes");
		String nodeName = nodes.fieldNames().next();
		JsonNode thisNode = nodes.get(nodeName);
		JsonNode plugins = thisNode.get("plugins");
		assertEquals(plugins.size(), 1);

		JsonNode plugin = plugins.get(0);
		assertEquals(plugin.get("name").asText(), ArangoDbRiverPlugin.NAME);
	}

	public static SearchResponse query(String esHost, int esPort, String esIndex, String esType, String keyword) throws IOException {
		Request req = new Request.Builder().url("http://" + esHost + ":" + esPort + "/" + esIndex + "/" + esType + "/_search?q=" + keyword).get().build();
		Response rsp = OKHTTP.newCall(req).execute();
		String body = rsp.body().string();
		LOG.debug("Query Response: %s", body);
		return MAPPER.readValue(body, SearchResponse.class);
	}
}
