package org.elasticsearch.river.arangodb.wal.states;

import static net.swisstech.swissarmyknife.lang.Threads.sleepFor;
import static org.elasticsearch.river.arangodb.wal.StateName.SLEEP;
import net.swisstech.swissarmyknife.lang.BoundedLong;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.BaseState;
import org.elasticsearch.river.arangodb.wal.StateMachine;

/**
 * reusable transient state, only removes itself from the stack but never decides on the next state. the state scheduling a Sleep must decide on the next state
 * beforehand.
 */
@Singleton
public class Sleep extends BaseState {

	private final BoundedLong waitTime;

	@Inject
	public Sleep(StateMachine stateMachine, ArangoDbConfig config) {
		super(stateMachine, config, SLEEP);
		long minWait = config.getArangodbReaderMinSleep();
		long maxWait = config.getArangodbReaderMaxSleep();
		waitTime = new BoundedLong(minWait, maxWait);
	}

	@Override
	public void execute() {

		/*
		 * do work
		 */

		long time = waitTime.get();
		sleepFor(time);

		/*
		 * next state
		 */

		// remove self, we'll return to the previous state or
		// whatever has been scheduled by the previous state
		stateMachine.pop();
	}

	public void resetErrorCount() {
		waitTime.setMin();
	}

	public void increaseErrorCount() {
		waitTime.multiply(2);
	}
}
