package org.elasticsearch.river.arangodb.wal;

import java.io.Closeable;

import net.swisstech.log.Logger;
import net.swisstech.log.LoggerFactory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class WalReaderRunnable implements Runnable, Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(WalReaderRunnable.class);

	private final StateMachine data;

	private boolean keepRunning;

	@Inject
	public WalReaderRunnable(StateMachine data) {
		this.data = data;
	}

	@Override
	public void run() {
		while (keepRunning) {
			State state = data.peek();
			if (state == null) {
				LOG.info("next state is null, exiting");
				break;
			}
			LOG.info("Executing state %s", state.getClass().getSimpleName());
			state.execute();
		}
	}

	@Override
	public void close() {
		keepRunning = false;
	}
}
