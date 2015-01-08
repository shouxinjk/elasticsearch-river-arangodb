
TODO / Refactorings
===================

* which java version to support? currently it's 1.6
* org.json
  * the old version is really old... the newer versions won't work with java6
* ReplogEntity
  * don't catch JSONException, it's an unchecked Exception (unless it causes problems)
  * see if we can deserialize the json fragments into a simple pojo instead of extending JSONObject

