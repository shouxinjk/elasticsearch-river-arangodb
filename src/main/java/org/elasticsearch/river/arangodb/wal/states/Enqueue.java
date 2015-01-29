package org.elasticsearch.river.arangodb.wal.states;

import static org.elasticsearch.river.arangodb.wal.StateName.ENQUEUE;
import static org.elasticsearch.river.arangodb.wal.StateName.READ_WAL;
import static org.elasticsearch.river.arangodb.wal.StateName.SLEEP;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.swisstech.arangodb.model.wal.WalDump;
import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class Enqueue extends BaseState {

	private static final ESLogger LOG = ESLoggerFactory.getLogger(Enqueue.class.getName());

	private WalDump data;

	@Inject
	public Enqueue(StateMachine stateMachine, ArangoDbConfig config) {
		super(stateMachine, config, ENQUEUE);
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		enqueue();

		/*
		 * next state
		 */

		stateMachine.pop();

		Sleep sleep = (Sleep) stateMachine.get(SLEEP);
		ReadWal readWal = (ReadWal) stateMachine.get(READ_WAL);

		// we will be reading the WAL again
		stateMachine.push(readWal);

		// we may need to sleep
		boolean checkMore = data.getHeaders().getReplicationCheckmore();
		if (!checkMore) {
			stateMachine.push(sleep);
		}
	}

	private void enqueue() {
		// TODO add another config value for how long we'll wait to write to the stream?
		long timeout = config.getIndexBulkTimeout();

		BlockingQueue<WalEvent> stream = config.getEventStream();
		for (WalEvent event:data.getEvents()) {
			try {
				stream.offer(event, timeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				String key = event.getKey();
				String tick = event.getTick();
				LOG.warn("Event key: " + key + " tick:" + tick + " could not be added to stream");
			}
		}
	}

	public void setData(WalDump data) {
		this.data = data;
	}
}
