package org.elasticsearch.river.arangodb;

import static net.swisstech.swissarmyknife.io.Closeables.close;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.river.arangodb.testclient.DebugClient;

/**
 * a main class to launch ES for debugging. Since the arangodb plugin is already in the classpath, we don't even need to install it, how convenient! Based on
 * this blog post: https://orrsella.com/2014/10/28/embedded-elasticsearch-server-for-scala-integration-tests/
 */
public class DebugMain {

	private static final String CLUSTER_NAME = "debugTestCluster";
	private static final String NODE_NAME = "debugTestNode";

	private final String basePath;
	private final Path baseDir;
	private final Path pluginsDir;
	private final Path confDir;
	private final Path logsDir;
	private final Path dataDir;
	private final Path workDir;

	public DebugMain() throws IOException {
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
		new DebugMain().execute();
	}

	public void execute() throws IOException {

		String arangoHost = "localhost";
		int arangoPort = 8529;
		String arangoDb = "_system";
		String collectionName = "test";

		String esHost = "localhost";
		int esPort = 9200;

		Settings settings = ImmutableSettings //
			.settingsBuilder() //
			.put("cluster.name", CLUSTER_NAME) //
			.put("node.name", NODE_NAME) //
			.put("http.port", esPort) //
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

		DebugClient.checkPluginInstalled(esHost, esPort);

		System.out.println(">>>");
		System.out.println(">>> START RIVER");
		System.out.println(">>>");

		DebugClient.createCollection(arangoHost, arangoPort, arangoDb, collectionName);

		System.out.println(">>>");
		System.out.println(">>> START RIVER");
		System.out.println(">>>");

		DebugClient.startRiver(arangoDb, collectionName, arangoHost, arangoPort, "arango_river", esHost, esPort, "test_index", "test_type");

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
					System.out.println("> exterminate!!");
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

		DebugClient.deleteCollection(arangoHost, arangoPort, arangoDb, collectionName);
		FileUtils.forceDelete(baseDir.toFile());
	}
}
