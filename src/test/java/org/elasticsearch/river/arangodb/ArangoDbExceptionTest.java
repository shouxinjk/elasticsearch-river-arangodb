package org.elasticsearch.river.arangodb;

import org.testng.annotations.Test;

/** test the ArangoException */
public class ArangoDbExceptionTest {

	/** this is just for unit test coverage */
	@Test
	public void testCoverage() {
		new ArangoDbException();
		new ArangoDbException("message");
		new ArangoDbException(new Exception());
		new ArangoDbException("message", new Exception());
	}
}
