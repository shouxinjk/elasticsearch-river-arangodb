package org.elasticsearch.river.arangodb.wal;

import java.util.HashMap;
import java.util.Map;

import net.swisstech.swissarmyknife.util.Stack;

import org.elasticsearch.river.arangodb.config.ArangoDbConfig;

public class StateMachine {

	private final Stack<State> states = new Stack<>();
	private final Map<StateName, State> map = new HashMap<>();

	private final ArangoDbConfig config;

	public StateMachine(ArangoDbConfig config) {
		this.config = config;
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
