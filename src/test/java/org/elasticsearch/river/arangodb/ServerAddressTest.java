package org.elasticsearch.river.arangodb;

import java.io.IOException;

import net.swisstech.swissarmyknife.test.DtoTesterUtil;

import org.testng.annotations.Test;

/** test the ServerAddress */
public class ServerAddressTest {

	/** this is just for unit test coverage */
	@Test
	public void testProperties() throws IOException {
		DtoTesterUtil.testAllProperties(new ServerAddress("localhost", 1));
	}
}
