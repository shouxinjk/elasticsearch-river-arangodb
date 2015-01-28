package org.elasticsearch.river.arangodb.wal.states;

import net.swisstech.arangodb.model.wal.WalDump;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

public class Enqueue extends BaseState {

	private final ReadWal readWal;
	private final Sleep sleep;

	public Enqueue(StateMachine stateMachine, ArangoDbConfig config, ReadWal readWal, Sleep sleep) {
		super(stateMachine, config);
		this.readWal = readWal;
		this.sleep = sleep;
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */
		WalDump dump = Globals.getDump();
		enqueue(dump);

		/*
		 * next state
		 */
		StateMachine sm = getStateMachine();
		sm.pop();

		// we will be reading the WAL again
		sm.push(readWal);

		// we may need to sleep
		boolean checkMore = dump.getHeaders().getReplicationCheckmore();
		if (!checkMore) {
			sm.push(sleep);
		}
	}

	private void enqueue(WalDump dump) {
		// TODO Auto-generated method stub
	}
}
