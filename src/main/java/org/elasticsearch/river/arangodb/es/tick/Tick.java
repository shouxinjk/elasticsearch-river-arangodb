package org.elasticsearch.river.arangodb.es.tick;

public class Tick {

	private Long tick;

	public Tick() {}

	public Tick(Long tick) {
		this.tick = tick;
	}

	public Long getTick() {
		return tick;
	}

	public void setTick(Long tick) {
		this.tick = tick;
	}
}
