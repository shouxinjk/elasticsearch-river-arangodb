package org.elasticsearch.river.arangodb;

import static net.swisstech.swissarmyknife.io.Closeables.close;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import net.swisstech.arangodb.MgmtClient;
import net.swisstech.arangodb.MgmtClient.CreateCollectionResponse;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugin.river.arangodb.ArangoDbRiverPlugin;
import org.elasticsearch.river.arangodb.client.Arangodb;
import org.elasticsearch.river.arangodb.client.Index;
import org.elasticsearch.river.arangodb.client.Meta;

import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * a main class to launch ES for debugging. Since the arangodb plugin is already in the classpath, we don't even need to install it, how convenient! Based on
 * this blog post: https://orrsella.com/2014/10/28/embedded-elasticsearch-server-for-scala-integration-tests/
 */
public class TestMain {

	private static final MediaType MEDIATYPE_JSON = MediaType.parse("application/json; charset=utf-8");

	private static final String CLUSTER_NAME = "debugTestCluster";
	private static final String NODE_NAME = "debugTestNode";
	private static final int HTTP_PORT = 19200;

	private final String basePath;
	private final Path baseDir;
	private final Path pluginsDir;
	private final Path confDir;
	private final Path logsDir;
	private final Path dataDir;
	private final Path workDir;

	public TestMain() throws IOException {
		basePath = "/tmp/debug_river_" + System.currentTimeMillis();
		baseDir = Paths.get(basePath);
		Files.createDirectory(baseDir);

		pluginsDir = Files.createDirectory(baseDir.resolve("plugins"));
		confDir = Files.createDirectory(baseDir.resolve("conf"));
		logsDir = Files.createDirectory(baseDir.resolve("logs"));
		dataDir = Files.createDirectory(baseDir.resolve("data"));
		workDir = Files.createDirectory(baseDir.resolve("work"));
	}

	public static void main(String[] args) throws IOException {
		new TestMain().execute();
	}

	public void execute() throws IOException {

		Settings settings = ImmutableSettings //
			.settingsBuilder() //
			.put("cluster.name", CLUSTER_NAME) //
			.put("node.name", NODE_NAME) //
			.put("http.port", HTTP_PORT) //
			.put("path.plugins", pluginsDir.toString()) //
			.put("path.conf", confDir.toString()) //
			.put("path.logs", logsDir.toAbsolutePath()) //
			.put("path.data", dataDir.toString()) //
			.put("path.work", workDir.toString()) //
			.build();

		Node node = NodeBuilder //
			.nodeBuilder() //
			.local(true) //
			.settings(settings) //
			.build();

		System.out.println(">>>");
		System.out.println(">>> START ELASTICSEARCH");
		System.out.println(">>>");

		node.start();

		System.out.println(">>>");
		System.out.println(">>> CREATE INDEX");
		System.out.println(">>>");

		try {
			AdminClient adm = node.client().admin();
			adm.indices().prepareCreate("test_index").get();
			adm.cluster().prepareHealth("test_index").setWaitForActiveShards(1);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println(">>>");
		System.out.println(">>> PLUGIN CHECK");
		System.out.println(">>>");

		checkPluginInstalled(node);

		System.out.println(">>>");
		System.out.println(">>> START RIVER");
		System.out.println(">>>");

		createCollection();

		System.out.println(">>>");
		System.out.println(">>> START RIVER");
		System.out.println(">>>");

		startRiver();

		System.out.println(">>>");
		System.out.println(">>> TYPE 'quit' TO STOP ELASTICSEARCH");
		System.out.println(">>>");
		System.out.flush();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = br.readLine();
				System.out.println("> " + line);
				if ("quit".equals(line)) {
					System.out.println("> bye bye");
					break;
				}
				if ("kill".equals(line)) {
					System.out.println("> kill kill kill!!!");
					return;
				}
			}
		}
		finally {
			close(br);
			node.stop();

			System.out.println("###");
			System.out.println("### WORKINGDIR WAS " + baseDir.toAbsolutePath().toString());
			System.out.println("###");
		}

		FileUtils.forceDelete(baseDir.toFile());
	}

	private void createCollection() throws IOException {
		MgmtClient mc = new MgmtClient("http://localhost:8529/_db/_system");
		CreateCollectionResponse ccrsp = mc.createCollection("test");
		int code = ccrsp.getCode();
		if (code != 200 && code != 409) {
			fail("creating collection in arangodb failed: " + new ObjectMapper().writeValueAsString(ccrsp));
		}
	}

	private void startRiver() throws IOException {

		Arangodb arangodb = new Arangodb();
		arangodb.setCollection("test");
		arangodb.setCredentials(null);
		arangodb.setDb("_system");
		arangodb.setDropCollection(false);
		arangodb.setExcludeFields(new ArrayList<String>());
		arangodb.setHost("localhost");
		arangodb.setPort(8529);
		arangodb.setReaderMaxSleep("500ms");
		arangodb.setReaderMinSleep("100ms");
		arangodb.setScript(null);

		Index index = new Index();
		index.setBulkSize(5000);
		index.setBulkTimeout("50ms");
		index.setName("test_index");
		index.setType("test_type");

		Meta meta = new Meta();
		meta.setArangodb(arangodb);
		meta.setIndex(index);

		String json = new ObjectMapper().writeValueAsString(meta);

		System.out.println("###");
		System.out.println("### REQ: " + json);
		System.out.println("###");

		RequestBody body = RequestBody.create(MEDIATYPE_JSON, json);
		Request req = new Request.Builder().url("http://localhost:" + HTTP_PORT + "/_river/arangodb_test/_meta").put(body).build();
		Response rsp = new OkHttpClient().newCall(req).execute();

		System.out.println("###");
		System.out.println("### RSP: " + rsp.body().string());
		System.out.println("###");

		assertEquals(rsp.code(), 201);
	}

	private void checkPluginInstalled(Node node) throws IOException {
		Request req = new Request.Builder().url("http://localhost:" + HTTP_PORT + "/_nodes").get().build();
		Response rsp = new OkHttpClient().newCall(req).execute();
		InputStream in = rsp.body().byteStream();
		JsonNode root = new ObjectMapper().readValue(in, JsonNode.class);

		JsonNode nodes = root.get("nodes");
		String nodeName = nodes.getFieldNames().next();
		JsonNode thisNode = nodes.get(nodeName);
		JsonNode plugins = thisNode.get("plugins");
		assertEquals(plugins.size(), 1);

		JsonNode plugin = plugins.get(0);
		assertEquals(plugin.get("name").asText(), ArangoDbRiverPlugin.NAME);
	}
}
