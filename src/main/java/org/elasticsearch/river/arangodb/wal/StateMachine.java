package org.elasticsearch.river.arangodb.wal;

import java.util.HashMap;
import java.util.Map;

import net.swisstech.arangodb.WalClient;
import net.swisstech.swissarmyknife.util.Stack;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.wal.states.CollectionCheck;
import org.elasticsearch.river.arangodb.wal.states.Enqueue;
import org.elasticsearch.river.arangodb.wal.states.ReadWal;
import org.elasticsearch.river.arangodb.wal.states.Sleep;

@Singleton
public class StateMachine {

	private final Stack<State> states = new Stack<>();
	private final Map<StateName, State> map = new HashMap<>();

	private final ArangoDbConfig config;

	@Inject
	public StateMachine(ArangoDbConfig config, WalClient client) {
		this.config = config;

		// initialize all states
		// TODO still need a cleaner solution for this state to statemachine and back circular dependency
		// should have them referenced from somewhere, preferrably as static fields but then we can't
		// use guice's ctor injection
		new CollectionCheck(this, config, client);
		new Enqueue(this, config);
		new ReadWal(this, config, client);
		new Sleep(this, config);
	}

	public State peek() {
		return states.peek();
	}

	public State pop() {
		return states.pop();
	}

	public void push(State state) {
		states.push(state);
	}

	public void push(StateName name) {
		push(get(name));
	}

	public State get(StateName name) {
		return map.get(name);
	}

	public void register(StateName name, State state) {
		map.put(name, state);
	}

	public ArangoDbConfig getConfig() {
		return config;
	}
}
