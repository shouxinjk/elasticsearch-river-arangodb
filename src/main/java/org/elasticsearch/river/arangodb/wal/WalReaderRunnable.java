package org.elasticsearch.river.arangodb.wal;

import static org.elasticsearch.river.arangodb.wal.StateName.COLLECTION_CHECK;

import java.io.Closeable;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

@Singleton
public class WalReaderRunnable implements Runnable, Closeable {

	private static final ESLogger LOG = ESLoggerFactory.getLogger(WalReaderRunnable.class.getName());

	private final StateMachine data;

	private boolean keepRunning = true;

	@Inject
	public WalReaderRunnable(StateMachine data) {
		this.data = data;
	}

	@Override
	public void run() {
		// initial state
		data.push(COLLECTION_CHECK);

		// run!
		while (keepRunning) {
			try {
				State state = data.peek();
				LOG.debug("Executing state {}", state);
				if (state == null) {
					LOG.warn("State is null, exiting");
					break;
				}
				state.execute();
			}
			catch (Exception e) {
				LOG.warn("Exception", e);
			}
		}

		LOG.info("Thread exiting");
	}

	@Override
	public void close() {
		keepRunning = false;
	}
}
