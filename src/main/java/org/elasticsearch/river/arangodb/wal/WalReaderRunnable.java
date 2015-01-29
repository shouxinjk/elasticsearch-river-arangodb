package org.elasticsearch.river.arangodb.wal;

import java.io.Closeable;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class WalReaderRunnable implements Runnable, Closeable {

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
				break;
			}
			state.execute();
		}
	}

	@Override
	public void close() {
		keepRunning = false;
	}
}
