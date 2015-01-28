package org.elasticsearch.river.arangodb.wal;

import java.io.Closeable;

import net.swisstech.swissarmyknife.util.Stack;

public class StateMachine implements Runnable, Closeable {

	private final Stack<State> states = new Stack<>();

	private boolean keepRunning;

	@Override
	public void run() {
		while (keepRunning) {
			State state = states.peek();
			if (state == null) {
				break;
			}
			state.execute();
		}
	}

	@Override
	public void close() {
		keepRunning = false;
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
}
