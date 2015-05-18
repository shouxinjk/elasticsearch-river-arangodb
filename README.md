# ArangoDB River Plugin for ElasticSearch

This is an ElasticSearch plugin that will connect to your ArangoDB server, read the Write-Ahead-Log of a collection and update an ElasticSearch index with the data.

# Build Status

| Branch | Status |
|--------|--------|
| Master | [![master](https://travis-ci.org/arangodb/elasticsearch-river-arangodb.svg?branch=master)](https://travis-ci.org/arangodb/elasticsearch-river-arangodb) |
| Devel  | [![devel](https://travis-ci.org/arangodb/elasticsearch-river-arangodb.svg?branch=devel)](https://travis-ci.org/arangodb/elasticsearch-river-arangodb) |

# Version Compatibility

| ArangoDB River Plugin | ArangoDB                                         | ElasticSearch  |
|-----------------------|--------------------------------------------------|----------------|
| 1.0.1                 | 2.6 and higher                                   | 1.4 and higher |
| 1.0.0.rc4             | 2.2 and higher                                   | 1.4 and higher |
| 0.3.0                 | up to 2.2 ( and higher if old replog is running) | 1.4.x          |
| 0.2.0                 | 1.4.0                                            | 1.0.0          |
| 0.1.0-alpha           | 1.4.0                                            | 0.90.5         |

The ArangoDB river artifact is named `elasticsearch-river-arangodb-<version>.jar`.

The 0.x versions are old and will not be maintained anymore. Please use the latest version and file bugs against that.

# Installation

The zip file contains all necessary dependencies and nothing else is needed to install it in ElasticSearch.

## Install from URL

```
/usr/share/elasticsearch/bin/plugin \
    --install arangodb \
    --url https://github.com/arangodb/elasticsearch-river-arangodb/releases/download/1.0.1/elasticsearch-river-arangodb-1.0.1.zip
```

## install from file (after manual download)

```
/usr/share/elasticsearch/bin/plugin \
    --install arangodb \
    --url file:///${HOME}/Downloads/elasticsearch-river-arangodb-1.0.1.zip
```

## Manual install

To install manually, unzip the archive into the ES plugins directory see: 'paths.plugin' config value of ES. The file structure should look like this:

```
$ cd <ES plugins directory>
$ find . -type f
./arangodb/elasticsearch-river-arangodb-1.0.1.jar
./arangodb/... more jars (dependencies of the plugin) ...
```

## Remove Plugin

Removing the plugin is required for upgrades.

```
/usr/share/elasticsearch/bin/plugin --remove arangodb
```

# Start & Configuration

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
curl -XPUT 'http://localhost:9200/_river/arangodb_test_car2/_meta' -d '{
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
        "reader_min_sleep": "100ms",
        "reader_max_sleep": "10s",
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

# Scripting

To use scripting to filter or manipulate documents before they are indexed, you must install the ElasticSearch scripting plugin. Please refer to the [ElasticSearch Plugins](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-plugins.html) documentation. Specifically, the scripting plugins.

# Tips & Tricks

## ArangoDB Authentication

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

## Filtering

Use scripting if you want to filter your data. Let's say your documents have a boolean field named "available".
The following script will filter your data due to their availability flags:

```
"script" : "if ( ctx.doc.available == false ) { ctx.operation = 'SKIP' };"
```

This script checks the "available" flag, and it changes the operation to "SKIP" for the given document context. This document will not be indexed. Please note that, if the document is already indexed, this will mean it won't be updated or deleted.

# TODO

* extend integration test:
  * stop river, make a change in arangodb, must not be reflected in ES anymore
  * start river again, should pick up where we stopped *or* start 'now' which means we've lost the last update

# Development & Contributing

If you want to change and test something, the basic development process is simple:

* fork and clone repo, checkout devel branch
* create your own branch from devel
* eclipse users run `gradle cleanEclipse eclipse`
* make your changes
* run the integration tests (requires ArangoDB and ElasticSearch to be installed) with `gradle clean test intTest`
* build a distribution with `gradle clean dist` (see directory `build/distributions`)
* test your custom build in your environment
* if happy commit and push, then create a pull request against the devel branch

# License

This software is licensed under the Apache 2 license, see the supplied LICENSE file.

# Changelog

## 1.0.1

- uses new version 1.1.8 or arangodb-wal-client, which supports new properties of the write ahead log that are introduced in arangodb 2.6

## 1.0.0.rc4

- Works with ArangoDB versions of 2.2 and higher
- Uses the Write-Ahead-Log and is *not* compatible with ArangoDB versions below 2.2
- This is a complete rewrite of the existing code
- Tested with ArangoDB 2.4 and ElasticSearch 1.4
- The configuration and featureset is slightly different. Existing users please carefully re-read the documentation

## 0.3.0

- Works with ArangoDB versions below 2.2
- Works with ArangoDB versions of 2.2 and higher if the legacy Replication Logger is enabled
- Update all dependencies to latest versions (ElasticSearch 1.4.x)
- Complete Refactoring, Cleanups, Simplifications

## 0.2.0

- Minor changes for ElasticSearch v1.0.0.

## 0.1.0-alpha

- Initial alpha version.
- Supports script filters.
- Using Apache HTTP standard components.
- No support yet for deleting or renaming collections.
- Based on former river layouts like CouchDB and MongoDB.
