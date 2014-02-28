tripod-php
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

Object Graph Mapper for managing RDF data in Mongo.

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
* Generic swiss army graph object ```ExtendedGraph``` which your application models can wrap or extend
* Supports transactions for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Named graph support
* Instruments using counters and timers to systems like graphite/carbon

[1] Prior to the MIT licened release we had supported an ElasticSearch search provider, this went beyond our initial (limited) requirements and was therefore stripped as we had no desire to maintain it.

Quickstart
----

```php
MongoTripodConfig::setConfig($conf); // set the config, usually read in as JSON from a file

$tripod = new MongoTripod(
  "CBD_users", // collection we're working with
  "usersdb" // db we're working with
);

// describe
$graph = $tripod->describe("http://example.com/user/1");
echo $graph->get_first_literal("http://example.com/user/1","http://xmlns.com/foaf/0.1/name"); 

// select
$data = $tripod->select(
  array("rdf:type.u"=>"http://xmlns.com/foaf/0.1/Person"),
  array("foaf:name"=>true);
);
if ($data['head']['count']>0) {
  foreach($data['results'] as $result) {
    echo $result['foaf:name'];
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

Requirements
----

PHP =>5.2, although not for long, future releases are soon to be >5.3.0

Mongo 2.x and up, although at least 2.2 is recommended to take advantage of database level locking, especially in the case of shared datasets.

We make use of the excellent [ARC](https://github.com/semsol/arc2) and elements of Tripod are based on the [Moriarty](https://code.google.com/p/moriarty/) library, the fruit of some earlier work by Talis to provide a PHP library for Talis' own proprietary cloud triple store (no longer in operation).

In production we run with datasets of > 500M triples over 70 databases on modest 3-node clusters (2 data nodes) with Dell R710 mid-range servers, 12 cores 96Gb RAM, RAID-10 array of non-SSD disks, m1.small arbitur in EC2.

Limitations
----

* Ad-hoc complex queries are a no as materialised views and tables are pre-specified. Changing these specifications requires that you re-generate materialised docments in their entirity.
* Relies heavy on namespaces. Inside the database predicates are always namespaced, infact it is a requirement to know and specify all your namespaces upfront in config, so working with arbitury unknown data is not a strong point
* Writes are expensive because they trigger invalidation of views and tables. The more view and table specifications you have, the slower writes are (or the slower data in views and tables becomes consistent, if you are backgrounding their generation)

What have you built with this?
----

The majority of the datasets underpinning [Talis Aspire](http://www.talis.com), an enterprise SaaS course management system serving 1M students in over 50 universities worldwide, are powered using graph data stored in MongoDB via the Tripod library.

We built tripod when we needed to migrate away from our own in-house proprietary triple store (incidently built around early versions of [Apache JENA](https://jena.apache.org/))

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

Transactions
----

MongoDB is only atomic at the document level. Tripod datasets store one [CBD](http://www.w3.org/Submission/CBD/)) per document. Therefore an update to a graph of data can impact 1..n documents.

Tripod maintains a transaction log (tlog) of updates to allow rollback in the case of multi-document writes. It is possible (and recommended) to run this on a seperate cluster to your main data. For disaster recovery, You can use the tlog to replay transactions on top of a known-good backup.

In production we run a small 2nd cluster in EC2 which stores up to 7 days of tlog, we prune and flush this periodically to S3. 

Coming soon (roadmap)
----

* Improvements to the background queue, currently this is a long running php script working of a queue held in mongo. Only ever intended for the PoC but it's still here 2 years later!
* An alternative persistence technology for the tlog. Memory mapped databases are not good for datasets with rapid turnover as the data files grow even if the data set is pruned. Implement a more specialist append-only database or even a RDBMS for the tlog persistence
* PHP >5.3.0 only. We still have some legacy servers on PHP 5.2 which is the only reason we continue support.
