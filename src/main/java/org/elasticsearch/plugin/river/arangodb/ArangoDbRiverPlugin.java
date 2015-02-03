/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.plugin.river.arangodb;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.arangodb.ArangoDbRiverModule;

public class ArangoDbRiverPlugin extends AbstractPlugin {

	public static final String NAME = "river-arangodb";
	public static final String DESC = "ArangoDB River Plugin";

	@Inject
	public ArangoDbRiverPlugin() {}

	@Override
	public String name() {
		return NAME;
	}

	@Override
	public String description() {
		return DESC;
	}

	public void onModule(RiversModule module) {
		module.registerRiver("arangodb", ArangoDbRiverModule.class);
	}
}
