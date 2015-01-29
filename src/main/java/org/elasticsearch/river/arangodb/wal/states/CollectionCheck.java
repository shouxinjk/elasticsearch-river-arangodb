package org.elasticsearch.river.arangodb.wal.states;

import static net.swisstech.swissarmyknife.lang.Numbers.tryParseLong;
import static org.elasticsearch.river.arangodb.wal.StateName.COLLECTION_CHECK;
import static org.elasticsearch.river.arangodb.wal.StateName.READ_WAL;
import static org.elasticsearch.river.arangodb.wal.StateName.SLEEP;

import java.io.IOException;

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

	@Inject
	public CollectionCheck(StateMachine stateMachine, ArangoDbConfig config, WalClient client) {
		super(stateMachine, config, COLLECTION_CHECK);
		this.client = client;
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		long tick = 0;
		boolean found = false;
		Inventory inventory = null;
		try {
			inventory = client.inventory();
		}
		catch (IOException e) {
			// this is bad! maybe just a temporary network error?
		}

		found = findCollection(inventory);
		tick = extractTick(inventory);

		/*
		 * next state
		 */

		Sleep sleep = (Sleep) stateMachine.get(SLEEP);
		ReadWal readWal = (ReadWal) stateMachine.get(READ_WAL);

		if (found) {
			sleep.resetErrorCount();

			// remove self
			stateMachine.pop();

			// next step is to read the WAL
			readWal.setTick(tick);
			stateMachine.push(readWal);
		}
		else {
			sleep.increaseErrorCount();
			stateMachine.push(sleep);
		}
	}

	private long extractTick(Inventory inventory) {
		String tick = inventory.getTick();
		return tryParseLong(tick, 0L);
	}

	private boolean findCollection(Inventory inventory) {
		String name = config.getArangodbCollection();
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
