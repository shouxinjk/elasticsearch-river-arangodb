package org.elasticsearch.river.arangodb.wal.states;

import net.swisstech.arangodb.WalClient;
import net.swisstech.arangodb.model.wal.WalDump;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

public class ReadWal extends BaseState {

	private final WalClient client;
	private final Enqueue enqueue;
	private final Sleep sleep;
	private final CollectionMissing collectionMissing;

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
		long lastTick = Globals.getLastTick();
		WalDump dump = client.dump(collName, lastTick);
		int code = dump.getResponseCode();

		/*
		 * next state
		 */

		StateMachine sm = getStateMachine();
		if (200 == code) {
			Globals.setLastTick(dump.getHeaders().getReplicationLastincluded());
			sleep.resetErrorCounter();
			enqueue.setData(dump);
			sm.pop();
			sm.push(enqueue);
		}
		else if (204 == code) {
			if (dump.getHeaders().getReplicationCheckmore()) {
				// no op, go straight back to Dump
			}
			else {
				sleep.increaseErrorCounter();
				sm.push(sleep)
			}
		}
		else if (404 == code) {
			sm.pop();
			sm.push(collectionMissing);
		}
	}
}
