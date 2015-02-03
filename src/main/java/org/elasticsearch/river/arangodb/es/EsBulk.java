package org.elasticsearch.river.arangodb.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.arangodb.es.tick.Tick;
import org.elasticsearch.river.arangodb.es.tick.Ticks;

@Singleton
public class EsBulk {

	private static final ESLogger LOG = ESLoggerFactory.getLogger(EsBulk.class.getName());

	private final Client client;
	private final Ticks ticks;

	private BulkRequestBuilder builder;

	@Inject
	public EsBulk(Client client, Ticks ticks) {
		this.client = client;
		this.ticks = ticks;
	}

	public void add(Tick tick) {
		IndexRequest tickUpdate = ticks.buildSaveReq(tick);
		builder.add(tickUpdate);
	}

	public int size() {
		if (builder == null) {
			return 0;
		}
		return builder.numberOfActions();
	}

	public void newBulkRequest() {
		builder = client.prepareBulk();
	}

	public BulkResponse executeBulk() {
		LOG.info("Executing bulk request with {} requests", size());
		BulkResponse resp = builder.get();
		newBulkRequest();
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
