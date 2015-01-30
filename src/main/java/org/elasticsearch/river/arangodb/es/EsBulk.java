package org.elasticsearch.river.arangodb.es;

import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.es.tick.Tick;
import org.elasticsearch.river.arangodb.es.tick.Ticks;

@Singleton
public class EsBulk {

	private final Client client;
	private final Ticks ticks;
	private final WalEventConverter converter;

	private BulkRequestBuilder builder;

	@Inject
	public EsBulk(Client client, Ticks ticks, WalEventConverter converter) {
		this.client = client;
		this.ticks = ticks;
		this.converter = converter;
	}

	public void add(Tick tick) {
		IndexRequest tickUpdate = ticks.buildSaveReq(tick);
		builder.add(tickUpdate);
	}

	/** @returns the tick of the event processed */
	public long add(WalEvent event) {
		// TODO
		return 0;
	}

	public int size() {
		return builder.numberOfActions();
	}

	public void newBulkRequest() {
		builder = client.prepareBulk();
	}

	public BulkResponse executeBulk() {
		BulkResponse resp = builder.get();
		builder = null;
		return resp;
	}

	public void add(IndexRequest request) {
		builder.add(request);
	}

	public void add(DeleteRequest request) {
		builder.add(request);
	}

	public void add(UpdateRequest request) {
		builder.add(request);
	}
}
