package org.elasticsearch.river.arangodb.wal.states;

import java.io.IOException;

import net.swisstech.arangodb.WalClient;
import net.swisstech.arangodb.model.wal.WalDump;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class ReadWal extends BaseState {

	private final WalClient client;
	private final Enqueue enqueue;
	private final Sleep sleep;
	private final CollectionMissing collectionMissing;

	private long lastTick = 0;

	@Inject
	public ReadWal(StateMachine stateMachine, ArangoDbConfig config, WalClient client, Enqueue enqueue, Sleep sleep, CollectionMissing collectionMissing) {
		super(stateMachine, config);
		this.client = client;
		this.enqueue = enqueue;
		this.sleep = sleep;
		this.collectionMissing = collectionMissing;
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		String collName = getConfig().getArangodbCollection();
		WalDump dump = null;
		int code = 0;
		try {
			dump = client.dump(collName, lastTick);
			code = dump.getResponseCode();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		/*
		 * next state
		 */

		StateMachine sm = getStateMachine();
		if (dump == null) {
			// this is bad! maybe just a temporary network error?
			sleep.increaseErrorCount();
			sm.push(sleep);
		}
		else if (200 == code) {
			lastTick = dump.getHeaders().getReplicationLastincluded();
			sleep.resetErrorCount();
			enqueue.setData(dump);
			sm.pop();
			sm.push(enqueue);
		}
		else if (204 == code) {
			if (dump.getHeaders().getReplicationCheckmore()) {
				// no op, go straight back to reading the WAL
			}
			else {
				sleep.increaseErrorCount();
				sm.push(sleep);
			}
		}
		else if (404 == code) {
			sm.pop();
			sm.push(collectionMissing);
		}
	}

	public void setLastTick(long lastTick) {
		this.lastTick = lastTick;
	}
}
