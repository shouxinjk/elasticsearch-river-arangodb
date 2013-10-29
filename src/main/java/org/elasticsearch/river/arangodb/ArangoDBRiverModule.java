package org.elasticsearch.river.arangodb;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class ArangoDBRiverModule extends AbstractModule {

  @Override protected void configure() {
    bind(River.class).to(ArangoDBRiver.class).asEagerSingleton();
  }
}