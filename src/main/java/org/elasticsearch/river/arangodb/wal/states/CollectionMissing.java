package org.elasticsearch.river.arangodb.wal.states;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

public class CollectionMissing extends BaseState {

	private final DropCollection dropCollection;
	private final CollectionCheck collectionCheck;

	public CollectionMissing(StateMachine stateMachine, ArangoDbConfig config, DropCollection dropCollection, CollectionCheck collectionCheck) {
		super(stateMachine, config);
		this.dropCollection = dropCollection;
		this.collectionCheck = collectionCheck;
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		boolean doDrop = getConfig().getArangodbOptionsDropcollection();

		/*
		 * next state
		 */

		StateMachine sm = getStateMachine();
		sm.pop();

		if (doDrop) {
			sm.push(dropCollection);
		}
		else {
			sm.push(collectionCheck);
		}
	}
}
