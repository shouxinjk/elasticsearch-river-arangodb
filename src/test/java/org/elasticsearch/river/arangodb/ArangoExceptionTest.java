package org.elasticsearch.river.arangodb;

/** test the ArangoException */
public class ArangoExceptionTest {

	/** this is just for unit test coverage */
	public void testCoverage() {
		new ArangoException();
		new ArangoException("message");
		new ArangoException(new Exception());
		new ArangoException("message", new Exception());
	}
}
