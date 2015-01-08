package org.elasticsearch.river.arangodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.json.JSONException;
import org.json.JSONObject;

public class ReplogEntity extends JSONObject {

	private static final ESLogger logger = ESLoggerFactory.getLogger(ReplogEntity.class.getName());

	private final ObjectMapper mapper = new ObjectMapper();

	public ReplogEntity(String str) throws JSONException {
		super(str);
	}

	public String getTick() {
		try {
			return (String) get("tick");
		}
		catch (ClassCastException e) {
			logger.error("error in ReplogEntity method 'getTick'", e);
			return null;
		}
		catch (JSONException e) {
			logger.error("error in ReplogEntity method 'getTick'", e);
			return null;
		}
	}

	public int getType() {
		try {
			return (Integer) get("type");
		}
		catch (ClassCastException e) {
			logger.error("error in ReplogEntity method 'getType'", e);
			return 0;
		}
		catch (JSONException e) {
			logger.error("error in ReplogEntity method 'getType'", e);
			return 0;
		}
	}

	public String getKey() {
		try {
			return (String) get("key");
		}
		catch (ClassCastException e) {
			logger.error("error in ReplogEntity method 'getKey'", e);
			return null;
		}
		catch (JSONException e) {
			logger.error("error in ReplogEntity method 'getKey'", e);
			return null;
		}
	}

	public String getRev() {
		try {
			return (String) get("rev");
		}
		catch (ClassCastException e) {
			logger.error("error in ReplogEntity method 'getRev'", e);
			return null;
		}
		catch (JSONException e) {
			logger.error("error in ReplogEntity method 'getRev'", e);
			return null;
		}
	}

	public Map<String, Object> getData() {
		try {
			if (has("data")) {
				return mapper.readValue(get("data").toString(), HashMap.class);
			}
			else {
				return null;
			}
		}
		catch (IOException e) {
			logger.error("Found IOException in ReplogEntity method 'getData'.");
			return null;
		}
		catch (JSONException e) {
			logger.error("Found JSONException in ReplogEntity method 'getData'.");
			return null;
		}
	}

	public String getOperation() {
		if (getType() == 2302) {
			return "DELETE";
		}
		else {
			return "UPDATE";
		}
	}
}
