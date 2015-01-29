package org.elasticsearch.river.arangodb.wal.states;

import static org.elasticsearch.river.arangodb.wal.StateName.COLLECTION_CHECK;
import static org.elasticsearch.river.arangodb.wal.StateName.COLLECTION_MISSING;
import static org.elasticsearch.river.arangodb.wal.StateName.DROP_COLLECTION;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class CollectionMissing extends BaseState {

	@Inject
	public CollectionMissing(StateMachine stateMachine, ArangoDbConfig config) {
		super(stateMachine, config, COLLECTION_MISSING);
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		boolean doDrop = config.getArangodbDropcollection();

		/*
		 * next state
		 */

		stateMachine.pop();

		if (doDrop) {
			DropCollection dropCollection = (DropCollection) stateMachine.get(DROP_COLLECTION);
			stateMachine.push(dropCollection);
		}
		else {
			CollectionCheck collectionCheck = (CollectionCheck) stateMachine.get(COLLECTION_CHECK);
			stateMachine.push(collectionCheck);
		}
	}
}
