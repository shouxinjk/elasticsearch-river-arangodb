package org.elasticsearch.river.arangodb;

import static net.swisstech.swissarmyknife.test.Assert.assertSize;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Map;

import org.json.JSONException;
import org.testng.annotations.Test;

public class ReplogEntityTest {

	@Test
	public void getReadOk() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"tick\":\"some tick\"}");
		assertEquals(re.getTick(), "some tick");
	}

	@Test
	public void getReadFailClassCast() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"tick\":1}");
		assertNull(re.getTick());
	}

	@Test
	public void getReadFailMissing() throws JSONException {
		ReplogEntity re = new ReplogEntity("{}");
		assertNull(re.getTick());
	}

	@Test
	public void getTypeOk() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"type\":1}");
		assertEquals(re.getType(), 1);
	}

	@Test
	public void getTypeFailClassCast() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"type\":\"hello\"}");
		assertEquals(re.getType(), 0);
	}

	@Test
	public void getTypeFailMissing() throws JSONException {
		ReplogEntity re = new ReplogEntity("{}");
		assertEquals(re.getType(), 0);
	}

	@Test
	public void getKeyOk() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"key\":\"some key\"}");
		assertEquals(re.getKey(), "some key");
	}

	@Test
	public void getKeyFailClassCast() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"key\":1}");
		assertNull(re.getKey());
	}

	@Test
	public void getKeyFailMissing() throws JSONException {
		ReplogEntity re = new ReplogEntity("{}");
		assertNull(re.getKey());
	}

	@Test
	public void getRevOk() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"rev\":\"some rev\"}");
		assertEquals(re.getRev(), "some rev");
	}

	@Test
	public void getRevFailClassCast() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"rev\":1}");
		assertNull(re.getRev());
	}

	@Test
	public void getRevFailMissing() throws JSONException {
		ReplogEntity re = new ReplogEntity("{}");
		assertNull(re.getRev());
	}

	@Test
	public void getDataOk() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"data\":{\"hello\":\"world\"}}");
		Map<String, Object> map = re.getData();
		assertSize(map, 1);
		assertEquals(map.get("hello"), "world");
	}

	@Test
	public void getDataFailStructure() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"data\":1}");
		assertNull(re.getData());
	}

	@Test
	public void getDataFailMissing() throws JSONException {
		ReplogEntity re = new ReplogEntity("{}");
		assertNull(re.getData());
	}

	@Test
	public void getOperationDelete() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"type\":2302}");
		assertEquals(re.getOperation(), "DELETE");
	}

	@Test
	public void getOperationUpdate() throws JSONException {
		ReplogEntity re = new ReplogEntity("{\"type\":1234}");
		assertEquals(re.getOperation(), "UPDATE");
	}
}
