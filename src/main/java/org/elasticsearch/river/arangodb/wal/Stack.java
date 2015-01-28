package org.elasticsearch.river.arangodb.wal;

import java.util.LinkedList;
import java.util.Queue;

/** simple non-vector based stack */
public class Stack<T> {

	private final Queue<T> elements = new LinkedList<>();

	public T peek() {
		return elements.peek();
	}

	public T pop() {
		return elements.remove();
	}

	public void push(T state) {
		elements.offer(state);
	}
}
