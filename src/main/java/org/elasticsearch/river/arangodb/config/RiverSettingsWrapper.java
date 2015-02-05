package org.elasticsearch.river.arangodb.config;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeByteValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeShortValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeTimeValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverSettings;

@Singleton
public class RiverSettingsWrapper {

	private final RiverSettings rs;

	@Inject
	public RiverSettingsWrapper(RiverSettings rs) {
		this.rs = rs;
	}

	public boolean getBool(String path) {
		return nodeBooleanValue(extract(path));
	}

	public boolean getBool(String path, boolean defaultValue) {
		return nodeBooleanValue(extract(path), defaultValue);
	}

	public byte getByte(String path) {
		return nodeByteValue(extract(path));
	}

	public byte getByte(String path, byte defaultValue) {
		return nodeByteValue(extract(path), defaultValue);
	}

	public double getDouble(String path) {
		return nodeDoubleValue(extract(path));
	}

	public double getDouble(String path, byte defaultValue) {
		return nodeDoubleValue(extract(path), defaultValue);
	}

	public float getFloat(String path) {
		return nodeFloatValue(extract(path));
	}

	public float getFloat(String path, byte defaultValue) {
		return nodeFloatValue(extract(path), defaultValue);
	}

	public int getInteger(String path) {
		return nodeIntegerValue(extract(path));
	}

	public int getInteger(String path, int defaultValue) {
		return nodeIntegerValue(extract(path), defaultValue);
	}

	public long getLong(String path) {
		return nodeLongValue(extract(path));
	}

	public long getLong(String path, long defaultValue) {
		return nodeLongValue(extract(path), defaultValue);
	}

	public short getShort(String path) {
		return nodeShortValue(extract(path));
	}

	public short getShort(String path, short defaultValue) {
		return nodeShortValue(extract(path), defaultValue);
	}

	public String getString(String path, String defaultValue) {
		return nodeStringValue(extract(path), defaultValue);
	}

	public TimeValue getTimeValue(String path) {
		return nodeTimeValue(extract(path));
	}

	public TimeValue getTimeValue(String path, TimeValue defaultValue) {
		return nodeTimeValue(extract(path), defaultValue);
	}

	public Map<String, Object> getMap(String path) {
		return getMap(path, Collections.<String, Object> emptyMap());
	}

	public Map<String, Object> getMap(String path, Map<String, Object> defaultValue) {
		Object value = extract(path);
		if (value == null) {
			return defaultValue;
		}
		return nodeMapValue(value, path);
	}

	public List<String> getList(String path) {
		return getList(path, Collections.<String> emptyList());
	}

	@SuppressWarnings("unchecked")
	public List<String> getList(String path, List<String> defaultValue) {
		Object value = extract(path);
		if (value == null) {
			return defaultValue;
		}
		return (List<String>) value;
	}

	public Object extract(String path) {
		return extractValue(path, rs.settings());
	}
}
