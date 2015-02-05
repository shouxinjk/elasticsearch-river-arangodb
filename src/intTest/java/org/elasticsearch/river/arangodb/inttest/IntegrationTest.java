package org.elasticsearch.river.arangodb.inttest;

import static net.swisstech.swissarmyknife.lang.Threads.sleepFor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import net.swisstech.arangodb.MgmtClient;
import net.swisstech.arangodb.MgmtClient.CreateDocumentResponse;
import net.swisstech.arangodb.MgmtClient.DeleteDocumentResponse;
import net.swisstech.log.Logger;
import net.swisstech.log.LoggerFactory;

import org.elasticsearch.river.arangodb.testclient.DebugClient;
import org.elasticsearch.river.arangodb.testclient.query.Hit;
import org.elasticsearch.river.arangodb.testclient.query.Hits;
import org.elasticsearch.river.arangodb.testclient.query.SearchResponse;
import org.elasticsearch.river.arangodb.testclient.query.TestDocument;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * basic integration test. there's much more one could test such as behaviour when a collection is missing or deleted while running etc. there are many untested
 * error cases. this test only does a basic check to verify the connection between arangodb and elasticsearch works and we can add/update/remove documents
 */
public class IntegrationTest {

	private static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

	@Test
	@Parameters({ "ARANGODB_PORT", "ELASTICSEARCH_PORT" })
	public void sunshineWeather(int arangoPort, int esPort) throws IOException {

		/*
		 * setup
		 */

		LOG.info(">>> Test setup");

		String arangoDb = "_system";
		String arangoCollection = "inttest_" + System.currentTimeMillis();
		String arangoHost = "localhost";
		String riverName = "arango_inttest";
		String esHost = "localhost";
		String indexName = "arango_inttest";
		String indexType = "arango_inttest";

		MgmtClient mc = new MgmtClient("http://" + arangoHost + ":" + arangoPort + "/_db/" + arangoDb);

		String keyword1 = "Donaudampfschifffahrtsgesellschaftskapitaenspatent";
		String keyword2 = "pneumonoultramicroscopicsilicovolcanoconiosis";

		// plugin must already be installed by inttest setup in gradle
		DebugClient.checkPluginInstalled(esHost, esPort);

		// start the river
		DebugClient.startRiver(arangoDb, arangoCollection, arangoHost, arangoPort, riverName, esHost, esPort, indexName, indexType);

		// create test collection in arangodb
		DebugClient.createCollection(arangoHost, arangoPort, arangoDb, arangoCollection);

		/*
		 * test
		 */

		// index must be available and zero hits for the keyword
		LOG.info(">>> Checking for ES index availability");
		long end = System.currentTimeMillis() + 10_000;
		boolean found = false;
		while (System.currentTimeMillis() < end) {
			SearchResponse sr = DebugClient.query(esHost, esPort, indexName, indexType, keyword1);
			int status = sr.getStatus();
			if (status == 404 || status == 503) {
				// not yet ready
				sleepFor(100);
				continue;
			}
			assertEquals(sr.getHits().getTotal(), 0);
			found = true;
			break;
		}

		assertTrue(found);

		// it seems we need to wait a little bit until the river is actually up and running. if we add the
		// document too early, the river plugin will connect after it's been sent to the WAL and we've
		// missed it... so we sleep a bit
		sleepFor(2_000);

		// add document
		LOG.info(">>> Adding doc to arangodb");
		TestDocument td = new TestDocument();
		td.setKey("testdoc_1");
		td.setText("hello " + keyword1 + " world");
		CreateDocumentResponse rsp = mc.create(arangoCollection, td);
		assertFalse(rsp.getError());
		// assertOneOf(rsp.getCode(), 201, 202);

		// check doc is in arangodb
		LOG.info(">>> Verifying doc is in arangodb");
		assertNotNull(mc.get(arangoCollection, td.getKey(), TestDocument.class));

		// 1 hit and matching content
		LOG.info(">>> Looking for added doc in elasticsearch");
		end = System.currentTimeMillis() + 10_000;
		found = false;
		while (System.currentTimeMillis() < end) {
			SearchResponse sr = DebugClient.query(esHost, esPort, indexName, indexType, keyword1);
			Hits hits = sr.getHits();
			if (hits.getTotal() != 1) {
				sleepFor(100);
				continue;
			}

			Hit hit = hits.getHits().iterator().next();
			assertEquals(hit.getSource().getKey(), td.getKey());
			assertEquals(hit.getSource().getText(), td.getText());
			found = true;
			break;
		}

		assertTrue(found);

		// update document
		LOG.info(">>> Updating existing document");
		td.setText("hello " + keyword2 + " world");
		rsp = mc.update(arangoCollection, td.getKey(), td);
		assertFalse(rsp.getError());
		// assertOneOf(rsp.getCode(), 201, 202);

		// 1 hit and matching content
		LOG.info(">>> Looking for updated doc in elasticsearch");
		end = System.currentTimeMillis() + 10_000;
		found = false;
		while (System.currentTimeMillis() < end) {
			SearchResponse sr = DebugClient.query(esHost, esPort, indexName, indexType, keyword2);
			Hits hits = sr.getHits();
			if (hits.getTotal() != 1) {
				sleepFor(100);
				continue;
			}

			Hit hit = hits.getHits().iterator().next();
			assertEquals(hit.getSource().getKey(), td.getKey());
			assertEquals(hit.getSource().getText(), td.getText());
			found = true;
			break;
		}

		assertTrue(found);

		// delete document
		LOG.info(">>> Deleting doc from arangodb");
		DeleteDocumentResponse del = mc.delete(arangoCollection, td.getKey());
		assertFalse(del.getError());
		// assertOneOf(del.getCode(), 200, 202);

		// index must be empty
		LOG.info(">>> Document must disappear from ES index");
		end = System.currentTimeMillis() + 10_000;
		found = true;
		while (System.currentTimeMillis() < end) {
			SearchResponse sr = DebugClient.query(esHost, esPort, indexName, indexType, keyword1);
			Hits hits = sr.getHits();
			if (hits.getTotal() == 1) {
				sleepFor(100);
				continue;
			}

			assertEquals(hits.getTotal(), 0);
			found = false;
			break;
		}

		assertFalse(found);

		/*
		 * teardown
		 */

		// delete collection
		LOG.info(">>> Cleanup");
		DebugClient.deleteCollection(arangoHost, arangoPort, arangoDb, arangoCollection);
	}
}
