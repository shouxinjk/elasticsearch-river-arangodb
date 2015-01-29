package org.elasticsearch.river.arangodb.wal.states;

import static org.elasticsearch.river.arangodb.wal.StateName.DROP_COLLECTION;
import static org.elasticsearch.river.arangodb.wal.StateName.SLEEP;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class DropCollection extends BaseState {

	@Inject
	public DropCollection(StateMachine stateMachine, ArangoDbConfig config) {
		super(stateMachine, config, DROP_COLLECTION);
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		dropCollection();

		/*
		 * next state
		 */

		stateMachine.pop();

		Sleep sleep = (Sleep) stateMachine.get(SLEEP);
		sleep.resetErrorCount();
		stateMachine.push(sleep);
	}

	private void dropCollection() {
		// TODO Auto-generated method stub
	}
}
