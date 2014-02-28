tripod-php
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

A library for managing RDF data in Mongo.

Features
----

* High performance single-ms query response time
* Production-hardened for datasets of 100 million triples and up
* Does not support SPARQL queries, instead SPARQL-like ```DESCRIBE/SELECT``` style operations are provided in two flavours
  * Ad-hoc queries where graph traversal is not required
  * Fixed-specification materialised views and tables provide extremely high performance graph traversal queries
* Trade speed with consistency on writes - views and tables can be updated immediately or in the background for eventual consistency
* Limited transaction support for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Modest hardware clusters can support > 500MT by sharding datasets
* Named graph support

