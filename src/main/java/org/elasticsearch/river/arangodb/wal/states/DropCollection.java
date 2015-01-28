package org.elasticsearch.river.arangodb.wal.states;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class DropCollection extends BaseState {

	private final Sleep sleep;

	@Inject
	public DropCollection(StateMachine stateMachine, ArangoDbConfig config, Sleep sleep) {
		super(stateMachine, config);
		this.sleep = sleep;
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

		StateMachine sm = getStateMachine();
		sm.pop();

		sleep.resetErrorCount();
		sm.push(sleep);
	}

	private void dropCollection() {
		// TODO Auto-generated method stub
	}
}
