package org.elasticsearch.river.arangodb.wal.states;

import net.swisstech.arangodb.model.wal.WalDump;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class Enqueue extends BaseState {

	private final ReadWal readWal;
	private final Sleep sleep;

	private WalDump data;

	@Inject
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
		enqueue();

		/*
		 * next state
		 */
		StateMachine sm = getStateMachine();
		sm.pop();

		// we will be reading the WAL again
		sm.push(readWal);

		// we may need to sleep
		boolean checkMore = data.getHeaders().getReplicationCheckmore();
		if (!checkMore) {
			sm.push(sleep);
		}
	}

	private void enqueue() {
		// TODO Auto-generated method stub
	}

	public void setData(WalDump data) {
		this.data = data;
	}
}
