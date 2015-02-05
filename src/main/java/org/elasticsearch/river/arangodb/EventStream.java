package org.elasticsearch.river.arangodb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.swisstech.arangodb.model.wal.WalEvent;

public class EventStream {

	private final BlockingQueue<WalEvent> queue;
	private final long writeTimeout;
	private final long readTimeout;

	public EventStream(BlockingQueue<WalEvent> queue, long writeTimeout, long readTimeout) {
		this.queue = queue;
		this.writeTimeout = writeTimeout;
		this.readTimeout = readTimeout;
	}

	public void offer(WalEvent event) throws InterruptedException {
		queue.offer(event, writeTimeout, TimeUnit.MILLISECONDS);
	}

	public WalEvent poll() throws InterruptedException {
		return queue.poll(readTimeout, TimeUnit.MILLISECONDS);
	}
}
