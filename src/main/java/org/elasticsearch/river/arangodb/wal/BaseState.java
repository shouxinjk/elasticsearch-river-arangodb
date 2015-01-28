package org.elasticsearch.river.arangodb.wal;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;

public abstract class BaseState implements State {

	private final StateMachine stateMachine;
	private final ArangoDbConfig config;

	public BaseState(StateMachine stateMachine, ArangoDbConfig config) {
		this.stateMachine = stateMachine;
		this.config = config;
	}

	public StateMachine getStateMachine() {
		return stateMachine;
	}

	public ArangoDbConfig getConfig() {
		return config;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
