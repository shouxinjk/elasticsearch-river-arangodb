ArangoDB River Plugin for ElasticSearch
=======================================

| ArangoDB River Plugin | ArangoDB | ElasticSearch |
|-----------------------|----------|---------------|
| master                | 1.4.0    | 1.0.0         |
| 0.2.0                 | 1.4.0    | 1.0.0         |
| 0.1.0-alpha           | 1.4.0    | 0.90.5        |

The ArangoDB river artefact is named `elasticsearch-river-arangodb-<version>.jar`.

Current artefact version is: `elasticsearch-river-arangodb-0.2.0.jar`.

Configuration
-------------

To create the river, run a curl statement from a shell:

```
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
```

Example:

```
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
```

Here is a more complex configuration with user credentials and a simple javascript example:

```
curl -XPUT 'http://localhost:9200/_river/arangodb_test_car/_meta' -d '{
    "type": "arangodb",
    "arangodb": {
        "host": "carhost",
        "port": carport,
        "db": "test",
        "collection": "car",
        "credentials": {
            "username": "riveruser",
            "password": "rivauser"
        },
        "options": {
            "drop_collection": false,
            "exclude_fields": [
                "internal1",
                "internal2"
            ]
        },
        "script" : "ctx.doc.title = ctx.doc.manufacturer + \" \" + ctx.doc.model;"
    },
    "index": {
        "name": "cars",
        "type": "car",
        "bulk_size": 5000,
        "bulk_timeout": "500ms"
    }
}'
```

Dependencies
------------

The following files have to be present when running the ArangoDB river.
Put them into the ArangoDB river plugin folder, together with the ArangoDB river artefact.
The standard ArangoDB river plugin folder location is: `<ES_HOME>/plugins/river_arangodb` (create this folder if it doesn't exist).
You can download a compressed file containing all of the required artefacts (see link below the artefacts lists).

#### Artifacts list for 0.2.0

- elasticsearch-river-arangodb-0.2.0.jar
- commons-logging-1.1.3.jar
- commons-codec-1.6.jar
- httpclient-4.3.1.jar
- httpcore-4.3.jar
- jackson-core-asl-1.9.4.jar
- jackson-mapper-asl-1.9.4.jar
- json-20090211.jar

Download: [`elasticsearch-river-arangodb-0.2.0.zip`](http://www.arangodb.org/downloads/elasticsearch-river-arangodb-0.2.0.zip).

Prerequisites
-------------

Before you can use the ArangoDB river, you must ask ArangoDB to switch into the replication logger mode.
To do so, open an ArangoDB shell and run the following commands:

```
db._useDatabase("<DATABASE_NAME>");
require("org/arangodb/replication").logger.properties({autoStart: true, maxEvents: 1048576 });
require("org/arangodb/replication").logger.start();
```

Don't forget to install the ElasticSearch language plugin if you intend to use a script within the river.
Example (javascript):

```
sudo <ES_HOME>/bin/plugin -install elasticsearch/elasticsearch-lang-javascript/2.2.0
```

(this is the current javascript language plugin version for ElasticSearch v1.2.1).
Scripting is not limited to javascript. See the corresponding ElasticSearch documentation.

Hints
-----

To add a user for basic authentication, open an ArangoDB shell and run the following command:

```
require("org/arangodb/users").save("<username>", "<password>");
```

This will create a user with the given user name and the given password.
Currently, ArangoDB just provides user authentication, but no authorization on a collection or operation level.

When starting the database, you can set the ArangoDB daemon parameter

```
--server.disable-authentication true
```

if you like to run the database without any authentication mechanism.

Filtering
----------

Use scripting if you want to filter your data. Let's say your documents have a boolean field named "available".
The following script will filter your data due to their availability flags:

```
"script" : "if ( ctx.doc.available == false ) { ctx.ignore = true };"
```

This script checks the "available" flag, and it adds an "ignore" flag to the given document context when indicated.
This "ignore" flag then makes the indexer skip the document when processing the streamed data.

License
-------

This software is licensed under the Apache 2 license, see the supplied LICENSE file.

Changelog
---------

#### 0.2.0
- Minor changes for ElasticSearch v1.0.0.

#### 0.1.0-alpha
- Initial alpha version.
- Supports script filters.
- Using Apache HTTP standard components.
- No support yet for deleting or renaming collections.
- Based on former river layouts like CouchDB and MongoDB.
