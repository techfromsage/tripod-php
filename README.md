tripod-php
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

A library for managing RDF data in Mongo.

Features
----

* High performance single-ms query response time on reasonable hardware
* Production-hardened for datasets of 100M triples and up
* Does not support SPARQL queries, instead SPARQL-like ```DESCRIBE/SELECT``` style operations are provided in two flavours
  * Ad-hoc queries where graph traversal is not required
  * Fixed-specification materialised views (```DESCRIBE```) or tables (```SELECT```) where graph traversal is required
* Trade speed with consistency on writes - views and tables can be updated immediately or in the background for eventual consistency
* (Very) simple search within datasets [1]
* Supports transactions for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Modest hardware clusters can support > 500M triples by sharding datasets
* Named graph support

[1] Prior to the MIT licened release we had supported an ElasticSearch search provider, this went beyond our initial (limited) requirements and was therefore stripped as we had no desire to maintain it.

Limitations
----

* Ad-hoc complex queries are a no as materialised views and tables are pre-specified. Changing these specifications requires that you re-generate materialised docments in their entirity.
* Relies heavy on namespaces. Inside the database predicates are always namespaced, infact it is a requirement to know and specify all your namespaces upfront in config, so working with arbitury unknown data is not a strong point
* Writes are expensive because they trigger invalidation of 

Why/when would I use this?
----

* You require high performance graph datastore 
* ...and the number of high performance complex queries required by your application is low (you'd be surprised how few complex queries an application might need)
* ...and the balance of your read/writes are in the order of >10:1

Why/when shouldn't I use this?
----

* You don't know the shape of your data
* You need to run ad-hoc graph traversal queries
* You need SPARQL support

Requirements
----

Mongo 2.x and up, although at least 2.2 is recommended to take advantage of database level locking, especially in the case of shared datasets.

In production we run with a dataset of > 500M triples over 50 databases on modest 3-node cluster (2 data nodes) with Dell R710 mid-range servers, 12 cores 96Gb RAM, RAID-10 array of non-SSD disks, m1.small arbitur in EC2.

Transactions
----

Tripod maintains a transaction log (tlog) of updates to allow rollback in the case of multi-document writes. It is possible (and recommended) to run this on a seperate cluster to your main data. For disaster recovery, You can use the tlog to replay transactions on top of a known-good backup.

In production we run a small 2nd cluster in EC2 which stores up to 7 days of tlog, we prune and flush this periodically to S3. 

