package org.elasticsearch.river.arangodb.es;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import net.swisstech.arangodb.model.wal.WalEvent;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.arangodb.EventStream;
import org.elasticsearch.river.arangodb.config.ArangoDbConfig;
import org.elasticsearch.river.arangodb.es.script.UserScript;
import org.elasticsearch.river.arangodb.es.tick.Tick;

@Singleton
public class IndexWriterRunnable implements Runnable, Closeable {

	private static final ESLogger LOG = ESLoggerFactory.getLogger(IndexWriterRunnable.class.getName());

	private final EventStream stream;
	private final int indexBulkSize;
	private final EsBulk bulk;
	private final UserScript userScript;
	private final ScriptResultProcessor processor;

	private boolean keepRunning = true;

	@Inject
	public IndexWriterRunnable(ArangoDbConfig config, EsBulk bulk, UserScript userScript, ScriptResultProcessor processor) {
		indexBulkSize = config.getIndexBulkSize();
		stream = config.getEventStream();
		this.bulk = bulk;
		this.userScript = userScript;
		this.processor = processor;
	}

	@Override
	public void run() {
		// initial request
		bulk.newBulkRequest();

		while (keepRunning) {
			try {
				doRun();
			}
			catch (Exception e) {
				LOG.error("Error", e);
			}
		}

		LOG.info("Thread exiting");
	}

	private void doRun() {

		long lastTickReceived = 0;
		WalEvent event = null;

		// read from the WAL's event stream and add them to the bulk request
		// up to the maximum configured request size
		while ((event = nextEvent()) != null) {
			lastTickReceived = event.getTick();

			// the user supplied script allows to optionally override behaviour
			// of the indexer or even the data to be indexed.
			Map<String, Object> ctx = userScript.executeScript(event);

			// check the ctx object and add to the bulk request if still
			// requested after the script has run.
			processor.process(ctx, bulk);

			// request is large enough, submit now
			if (bulk.size() > indexBulkSize) {
				LOG.trace("Bulk request full, executing");
				break;
			}
		}

		// nothing to do, go back to reading from the stream, which will
		// block for at most 'indexBukTimeout' so no need to add a sleep here.
		if (bulk.size() < 1) {
			LOG.trace("Bulk request empty, not executing");
			return;
		}

		// append another request to write the value of the last tick received
		// to ElasticSearch for tracking and later reading if we're restarted
		bulk.add(new Tick(lastTickReceived));

		// execute the request
		BulkResponse resp = bulk.executeBulk();
		// TODO process/analyze response
	}

	private WalEvent nextEvent() {
		try {
			return stream.poll();
		}
		catch (InterruptedException e) {
			LOG.warn("InterruptedException", e);
			// TODO Thread.currentThread().interrupt(); ???
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		LOG.info("close()");
		keepRunning = false;
	}
}
