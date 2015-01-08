package org.elasticsearch.river.arangodb;

public class ArangoException extends Exception {

	private static final long serialVersionUID = -6952243232153693602L;

	public ArangoException() {
		super();
	}

	public ArangoException(String message) {
		super(message);
	}

	public ArangoException(String message, Throwable cause) {
		super(message, cause);
	}

	public ArangoException(Throwable cause) {
		super(cause);
	}
}
