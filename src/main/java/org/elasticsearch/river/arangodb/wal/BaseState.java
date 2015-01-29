package org.elasticsearch.river.arangodb.wal;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;

public abstract class BaseState implements State {

	protected final StateMachine stateMachine;
	protected final ArangoDbConfig config;

	public BaseState(StateMachine stateMachine, ArangoDbConfig config, StateName name) {
		this.stateMachine = stateMachine;
		this.config = config;
		stateMachine.register(name, this);
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
