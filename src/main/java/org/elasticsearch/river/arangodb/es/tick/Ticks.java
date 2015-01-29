package org.elasticsearch.river.arangodb.es.tick;

import static org.elasticsearch.river.arangodb.util.JacksonUtil.MAPPER;

import java.io.IOException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;

@Singleton
public class Ticks {

	private final Client client;
	private final String index;
	private final String type;
	private final String id;

	@Inject
	public Ticks(Client client, ArangoDbConfig config) {
		this.client = client;
		index = config.getRiverIndexName();
		type = config.getRiverName();
		id = config.getArangodbCollection();
	}

	/**
	 * create a save request, we don't execute it here because this is intended to be appended to the bulk request as the last part. this will ensure the stored
	 * tick to be accurate with what has been indexed
	 */
	public IndexRequest buildSaveReq(Tick tick) throws IOException {
		if (tick == null) {
			return null;
		}
		String json = MAPPER.writeValueAsString(tick);
		return client //
			.prepareIndex(index, type, id) //
			.setSource(json) //
			.request();
	}

	/** get the last tick from ES */
	public Tick getLastTick() {
		GetResponse get = client //
			.prepareGet(index, type, id) //
			.get();
		if (get.isSourceEmpty()) {
			return null;
		}
		byte[] bytes = get.getSourceAsBytes();
		try {
			return MAPPER.readValue(bytes, Tick.class);
		}
		catch (IOException e) {
			return null;
		}
	}
}
