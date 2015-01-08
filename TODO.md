
TODO / Refactorings
===================

* which java version to support? currently it's 1.6
* org.json
  * the old version is really old... the newer versions won't work with java6
* ReplogEntity
  * don't catch JSONException, it's an unchecked Exception (unless it causes problems)
  * see if we can deserialize the json fragments into a simple pojo instead of extending JSONObject
  * getOperation only returns DELETE, UPDATE but the rest of the code also works with INSERT
* OpType
  * compare with [c code](https://github.com/triAGENS/ArangoDB/blob/master/arangod/VocBase/replication-common.h) and rename/split if necessary so it's correct. also, parsing should be moved out of ReplogEntity.
