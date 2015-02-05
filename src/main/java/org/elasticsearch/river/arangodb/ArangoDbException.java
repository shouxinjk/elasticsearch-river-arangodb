package org.elasticsearch.river.arangodb;

public class ArangoDbException extends Exception {

	private static final long serialVersionUID = -6952243232153693602L;

	public ArangoDbException() {
		super();
	}

	public ArangoDbException(String message) {
		super(message);
	}

	public ArangoDbException(String message, Throwable cause) {
		super(message, cause);
	}

	public ArangoDbException(Throwable cause) {
		super(cause);
	}
}
