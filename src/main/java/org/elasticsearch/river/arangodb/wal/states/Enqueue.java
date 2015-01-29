package org.elasticsearch.river.arangodb.wal.states;

import static org.elasticsearch.river.arangodb.wal.StateName.ENQUEUE;
import static org.elasticsearch.river.arangodb.wal.StateName.READ_WAL;
import static org.elasticsearch.river.arangodb.wal.StateName.SLEEP;
import net.swisstech.arangodb.model.wal.WalDump;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class Enqueue extends BaseState {

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

		// we will be reading the WAL again
		ReadWal readWal = (ReadWal) stateMachine.get(READ_WAL);
		stateMachine.push(readWal);

		// we may need to sleep
		boolean checkMore = data.getHeaders().getReplicationCheckmore();
		if (!checkMore) {
			Sleep sleep = (Sleep) stateMachine.get(SLEEP);
			stateMachine.push(sleep);
		}
	}

	private void enqueue() {
		// TODO Auto-generated method stub
	}

	public void setData(WalDump data) {
		this.data = data;
	}
}
