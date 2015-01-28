package org.elasticsearch.river.arangodb.wal.states;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

/**
 * reusable transient state, only removes itself from the stack but never decides on the next state. the state scheduling a Sleep must decide on the next state
 * beforehand
 */
public class Sleep extends BaseState {

	public Sleep(StateMachine stateMachine, ArangoDbConfig config) {
		super(stateMachine, config);
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		doSleep();

		/*
		 * next state
		 */

		getStateMachine().pop();
	}
}
