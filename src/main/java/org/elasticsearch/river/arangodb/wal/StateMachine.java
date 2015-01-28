package org.elasticsearch.river.arangodb.wal;

import net.swisstech.swissarmyknife.util.Stack;

public class StateMachine {

	private final Stack<State> states = new Stack<>();

	public State peek() {
		return states.peek();
	}

	public State pop() {
		return states.pop();
	}

	public void push(State state) {
		states.push(state);
	}
}
