ArangoDB River Plugin for ElasticSearch
=======================================

| ArangoDB River Plugin    | ArangoDB | ElasticSearch |
|--------------------------|----------|---------------|
| master                   | 1.4.0    | 0.90.5        |
| 1.0.0                    | 1.4.0    | 0.90.5        |


The ArangoDB river artefact is named `elasticsearch-river-arangodb-<version>.jar`.
Current artefact version: `elasticsearch-river-arangodb-1.0.0.jar`.

Configuration
-------------

To create the river, run a curl statement from a shell:

    curl -XPUT 'http://localhost:9200/_river/arangodb/_meta' -d '{
        "type": "arangodb", 
        "arangodb": { 
            "db": "<DATABASE_NAME>", 
            "collection": "<COLLECTION>", 
        }, 
        "index": { 
            "name": "<ES_INDEX_NAME>", 
            "type": "<ES_TYPE_NAME>" 
        }
    }'

Example:

    curl -XPUT 'http://localhost:9200/_river/arangodb_test_car/_meta' -d '{ 
        "type": "arangodb", 
        "arangodb": { 
            "db": "test", 
            "collection": "car"
        }, 
        "index": {
            "name": "cars", 
            "type": "car" 
        }
    }'
    
Here is a more complex configuration with a simple javascript example:
    
    curl -XPUT 'http://localhost:9200/_river/arangodb_test_car/_meta' -d '{ 
        "type": "arangodb", 
        "arangodb": { 
            "host": "carhost",
            "port": carport,
            "db": "test", 
            "collection": "cars",
            "credentials": {
                "username": "riveruser",
                "password": "rivauser"
            },
            "script" : "ctx.doc.additional = ctx.doc.manufacturer + \" + \" + ctx.doc.model;"
        }, 
        "index": {
            "name": "cars", 
            "type": "car" 
        }
    }'

Dependencies
------------

The following files have to be present when running the ArangoDB river. 
Put them into the ArangoDB river plugin folder, together with the ArangoDB river artefact.
Standard plugin folder location: `<ES_HOME>/plugins/river_arangodb`.

#### artefacts list for 1.0.0

- elasticsearch-river-arangodb-1.0.0.jar
- httpclient-4.3.1.jar  
- httpcore-4.3.jar  
- jackson-core-asl-1.9.4.jar  
- jackson-mapper-asl-1.9.4.jar  
- json-20090211.jar
        
Remarks
-------

Before you can use the ArangoDB river, you must ask ArangoDB to switch into the replication logger mode.
To do so, open an ArangoDB shell and run the following commands:

    require("org/arangodb/replication").logger.properties({autoStart: true, maxEvents: 1048576 });
    require("org/arangodb/replication").logger.start();

Don't forget to install the elasticsearch language plugin if you intend to use a script within the river.
Example (javascript):

    sudo <ES_HOME>/bin/plugin -install elasticsearch/elasticsearch-lang-javascript/1.2.0
  
To add a user for basic authentication, open an ArangoDB shell and run the following command:

    require("org/arangodb/users").save("<username>", "<password>");

This will create user <username> with the given password. 
You can set the ArangoDB daemon parameter

    --server.disable-authentication true

if you like to run the database without any authentication mechanism.
Currently, ArangoDB just provides user authentication, but no authorization on a collection or operation level.

License
-------

This software is licensed under the Apache 2 license, see the supplied LICENSE file.

Changelog
---------

#### 1.0.0 
- Initial version.
- Script filters.
- Using Apache HTTP standard components.
- Based on former river layouts like CouchDB and MongoDB.
