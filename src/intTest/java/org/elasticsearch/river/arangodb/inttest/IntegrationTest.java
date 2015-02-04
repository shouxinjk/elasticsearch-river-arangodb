package org.elasticsearch.river.arangodb.inttest;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class IntegrationTest {

	@Test
	@Parameters({ "ARANGODB_PORT", "ELASTICSEARCH_PORT" })
	public void test() {

	}
}
