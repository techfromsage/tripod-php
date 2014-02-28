tripod-php
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

A library for managing RDF data in Mongo.

Features
----

* High performance single-ms query response time
* Production-hardened for datasets of 100M triples and up
* Does not support SPARQL queries, instead SPARQL-like ```DESCRIBE/SELECT``` style operations are provided in two flavours
  * Ad-hoc queries where graph traversal is not required
  * Fixed-specification materialised views where graph traversal is required
* Trade speed with consistency on writes - views and tables can be updated immediately or in the background for eventual consistency
* Limited transaction support for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Modest hardware clusters can support > 500M triples by sharding datasets
* Named graph support

Requirements
----

Mongo 2.x and up, although at least 2.2 is recommended to take advantage of database level locking, especially in the case of shared datasets.

We have run for 2 years in production with a dataset of > 500M triples over 50 databases on modest 3-node cluster (2 data nodes) with Dell commodity servers, 24 cores 96Gb RAM, RAID-10 array of non-SSD disks.

Transactions
----

Tripod maintains a transaction log (tlog) of updates to allow rollback in the case of multi-document writes. It is possible (and recommended) to run this on a seperate cluster to your main data. For disaster recovery, You can use the tlog to replay transactions on top of a known-good backup.

