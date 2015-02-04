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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

@Singleton
public class CollectionCheck extends BaseState {

	private static final ESLogger LOG = ESLoggerFactory.getLogger(CollectionCheck.class.getName());

	private final WalClient client;
	private final String collectionName;

	@Inject
	public CollectionCheck(StateMachine stateMachine, ArangoDbConfig config, WalClient client) {
		super(stateMachine, config, COLLECTION_CHECK);
		this.client = client;
		collectionName = config.getArangodbCollection();
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		Inventory inventory = null;
		try {
			inventory = client.inventory();
		}
		catch (IOException e) {
			LOG.warn("Error getting inventory", e);
			// this is bad! maybe just a temporary network error?
		}


		/*
		 * next state
		 */

		Sleep sleep = (Sleep) stateMachine.get(SLEEP);
		ReadWal readWal = (ReadWal) stateMachine.get(READ_WAL);

		if (inventory == null) {
			// error, retry later
			sleep.increaseErrorCount();
			stateMachine.push(sleep);
		}
		else {
			boolean found = findCollection(inventory);
			if (found) {
				long tick = extractTick(inventory);

				LOG.info("Collection {} found, using tick {}", collectionName, tick);

				sleep.resetErrorCount();

				// remove self
				stateMachine.pop();

				// next step is to read the WAL
				readWal.setTick(tick);
				stateMachine.push(readWal);
			}
			else {
				LOG.info("Collection {} not found", collectionName);

				sleep.increaseErrorCount();
				stateMachine.push(sleep);
			}
		}
	}

	private long extractTick(Inventory inventory) {
		String tick = inventory.getTick();
		return tryParseLong(tick, 0L);
	}

	private boolean findCollection(Inventory inventory) {
		for (ArangoDbCollection collection : inventory.getCollections()) {
			CollectionParameters params = collection.getParameters();
			String collName = params.getName();
			if (collectionName.equals(collName)) {
				return true;
			}
		}
		return false;
	}
}
