tripod-php
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

Object Graph Mapper for managing RDF data in Mongo.

Quickstart
----

```php
MongoTripodConfig::setConfig($conf); // set the config

// describe
$tripod = new MongoTripod("CBD_users","usersdb");
$graph = $tripod->describe("http://example.com/user/1");
echo $graph->get_first_literal("http://example.com/user/1","http://xmlns.com/foaf/0.1/name"); 

// select
$data = $tripod->select(
  array("_id.r"=>"http://example.com/user/1"),
  array("foaf:name"=>true);
);
if ($data['head']['count']>0) {
  foreach($data['results'] as $result) {
    echo $data['results']['foaf:name'];
  }
}


// save
$newGraph = new ExtendedGraph();
$newGraph->add_literal_value("http://example.com/user/2","http://xmlns.com/foaf/0.1/name","John Smith");
$tripod->saveChanges(
  new ExtendedGraph(), // the before state, here there was no before (new data)
  $newGraph // the desired after state
);

```

Features
----

* High performance single-ms query response time on reasonable hardware with datasets >100MT
* Does not support SPARQL queries, instead SPARQL-like ```DESCRIBE/SELECT``` style operations are provided in two flavours
  * Ad-hoc queries where graph traversal is not required
  * Fixed-specification materialised views (```DESCRIBE```) or tables (```SELECT```) where graph traversal is required
* Trade speed with consistency on writes - views and tables can be updated immediately or in the background for eventual consistency
* Define indexes on predicates to speed up queries
* Page tabular data with counts multi-value cells (hurrah!)
* (Very) simple search within datasets [1]
* Supports transactions for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Named graph support
* Instruments using counters and timers to systems like graphite/carbon

[1] Prior to the MIT licened release we had supported an ElasticSearch search provider, this went beyond our initial (limited) requirements and was therefore stripped as we had no desire to maintain it.

Requirements
----

Mongo 2.x and up, although at least 2.2 is recommended to take advantage of database level locking, especially in the case of shared datasets.

In production we run with datasets of > 500M triples over 70 databases on modest 3-node clusters (2 data nodes) with Dell R710 mid-range servers, 12 cores 96Gb RAM, RAID-10 array of non-SSD disks, m1.small arbitur in EC2.

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
* ...your datasets are probably in the range of no more than 100MT each (we've not tested beyond this)

Why/when shouldn't I use this?
----

* You don't know the shape of your data
* You need to run ad-hoc graph traversal queries
* You need SPARQL support

What have you built with this?
----

The majority of the datasets underpinning [Talis Aspire](http://www.talis.com), an enterprise SaaS course management system serving 1M students in over 50 universities worldwide, are powered using graph data stored in MongoDB via the Tripod library.

Transactions
----

MongoDB is only atomic at the document level. Tripod datasets store one [CBD](http://www.w3.org/Submission/CBD/)) per document. Therefore an update to a graph of data can impact 1..n documents.

Tripod maintains a transaction log (tlog) of updates to allow rollback in the case of multi-document writes. It is possible (and recommended) to run this on a seperate cluster to your main data. For disaster recovery, You can use the tlog to replay transactions on top of a known-good backup.

In production we run a small 2nd cluster in EC2 which stores up to 7 days of tlog, we prune and flush this periodically to S3. 

