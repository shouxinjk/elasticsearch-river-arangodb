package org.elasticsearch.river.arangodb;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

import java.util.List;

import org.elasticsearch.river.RiverSettings;

public class RiverSettingsWrapper {

	private final RiverSettings rs;

	public RiverSettingsWrapper(RiverSettings rs) {
		this.rs = rs;
	}

	public String getString(String path, String defaultValue) {
		return nodeStringValue(extract(path), defaultValue);
	}

	public int getInt(String path, int defaultValue) {
		return nodeIntegerValue(extract(path), defaultValue);
	}

	public boolean getBool(String path, boolean defaultValue) {
		return nodeBooleanValue(extract(path), defaultValue);
	}

	public <T> List<T> getList(String path, List<T> defaultValue) {
		Object value = extract(path);
		if (value == null) {
			return defaultValue;
		}
		return (List<T>) value;
	}

	private Object extract(String path) {
		return extractValue(path, rs.settings());
	}
}
