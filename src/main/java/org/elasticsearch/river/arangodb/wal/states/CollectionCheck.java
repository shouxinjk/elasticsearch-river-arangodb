package org.elasticsearch.river.arangodb.wal.states;

import net.swisstech.arangodb.WalClient;
import net.swisstech.arangodb.model.ArangoDbCollection;
import net.swisstech.arangodb.model.CollectionParameters;
import net.swisstech.arangodb.model.Inventory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class CollectionCheck extends BaseState {

	private final WalClient client;
	private final ReadWal readWal;
	private final Sleep sleep;

	@Inject
	public CollectionCheck(StateMachine stateMachine, ArangoDbConfig config, WalClient client, ReadWal readWal, Sleep sleep) {
		super(stateMachine, config);
		this.client = client;
		this.readWal = readWal;
		this.sleep = sleep;
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		boolean found = collectionFound();

		/*
		 * next state
		 */

		StateMachine sm = getStateMachine();
		if (found) {
			sleep.resetErrorCounter();

			// remove self
			sm.pop();

			// next step is to read the WAL
			sm.push(readWal);
		}
		else {
			sleep.increaseErrorCounter();
			sm.push(sleep);
		}
	}

	private boolean collectionFound() {
		String name = getConfig().getArangodbCollection();
		Inventory inventory = client.inventory();
		for (ArangoDbCollection collection : inventory.getCollections()) {
			CollectionParameters params = collection.getParameters();
			String collName = params.getName();
			if (name.equals(collName)) {
				return true;
			}
		}

		return false;
	}
}
