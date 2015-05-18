![tripod-php](logo.png)
==========

![Alt desc](http://95.138.143.82:3002/builds/TRI/status.png)

Object Graph Mapper for managing [RDF](http://www.w3.org/TR/rdf-primer/) data stored in [MongoDB](http://www.mongodb.org/). See also [tripod-node](https://github.com/talis/tripod-node).

Features
----

* High performance single-ms query response time on reasonable hardware with datasets >100M triples
* Does not support SPARQL queries, instead SPARQL-like ```DESCRIBE/SELECT``` style operations are provided in two flavours
  * Ad-hoc queries where graph traversal is not required
  * Via Composites, which are fixed-specification materialised documents, supporting graph traversal. Presently there are three types of composite:
    * Views (```DESCRIBE```) - these are multi-subject graphs retrievable in one self-contained document
    * Tables (```SELECT```) - tabular datasets
    * Search - also tabular, more suitabe for search use cases
* Trade speed with consistency on writes - views and tables can be updated immediately or in the background for eventual consistency
* Define indexes on predicates to speed up queries
* Page tabular data with counts multi-value cells (hurrah!)
* (Very) simple search within datasets. Prior to the public release we supported an ElasticSearch search provider, this went beyond our requirements and was removed as we had no desire to maintain it.
* Generic swiss army graph object ```ExtendedGraph``` which your application models can wrap or extend
* Supports [transactions](https://github.com/talis/tripod-php/blob/master/README.md#transactions) for updates over multiple [CBDs](http://www.w3.org/Submission/CBD/)
* Named graph support
* Instruments using counters and timers to systems like graphite/carbon

Quickstart
----

```php
require_once("tripod.inc.php");

Config::setConfig($conf); // set the config, usually read in as JSON from a file

$tripod = new Tripod(
  "CBD_users", // pod (read: MongoDB collection) we're working with
  "myapp" // store (read: MongoDB database)  we're working with
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

// an expensive pre-defined graph traversal query
$graph = $tripod->getViewForResource("http://example.com/users","v_users");
$allUsers = $graph->get_subjects_of_type("http://xmlns.com/foaf/0.1/Person");

// save
$newGraph = new ExtendedGraph();
$newGraph->add_literal_value("http://example.com/user/2","http://xmlns.com/foaf/0.1/name","John Smith");
$tripod->saveChanges(
  new ExtendedGraph(), // the before state, here there was no before (new data)
  $newGraph // the desired after state
);

// save, but background all the expensive view/table/search generation
$tripod = new Tripod("CBD_users",  "usersdb", array(
    'async' = array(OP_VIEWS,OP_TABLES,OP_SEARCH) // async opt says what to do later via a queue rather than as part of the save
  )
);
$tripod->saveChanges(
  new ExtendedGraph(), // the before state, here there was no before (new data)
  $newGraph // the desired after state
);

```

Requirements
----

PHP >= 5.3.x

Mongo 2.x and up, although at least 2.2 is recommended to take advantage of database level locking, especially in the case of shared datasets.

MongoPHP driver version 1.3.4 and up


What does the config look like?
----

[Read the full docs](/docs/config.md)

Before you can do anything with tripod you need to initialise the config via the ```Config::setConfig()``` method. This takes an associative array which can generally be decoded from a JSON string. Here's an example:

```javascript
{
    "namespaces" : {
        "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "foaf":"http://xmlns.com/foaf/0.1/",
        "exampleapp":"http://example.com/properties/"
    },
    "defaultContext":"http://talisaspire.com/",
    "data_sources" : {
        "cluster1": {
            "type": "mongo",
            "connection": "mongodb:\/\/localhost",
            "replicaSet": ""
        },
        "cluster2": {
            "type": "mongo",
            "connection": "mongodb:\/\/othermongo.example.com",
            "replicaSet": ""
        }
    },
    "stores" : {
        "myapp" : {
            "data_source" : "cluster1",
            "pods" : {
                "CBD_users" : {
                    "cardinality" : {
                        "foaf:name" : 1
                    },
                    "indexes" : {
                        "names": {
                            "foaf:name.l":1
                        }
                    }
                }
            }
        },
        "view_specifications" : [
            {
                "_id": "v_users",
                "from":"CBD_users",
                "type": "exampleapp:AllUsers",
                "include": ["rdf:type"],
                "joins": {
                    "exampleapp:hasUser": {
                        "include": ["foaf:name","rdf:type"]
                        "joins": {
                            "foaf:knows" : {
                            "include": ["foaf:name","rdf:type"]
                            }
                        }
                    }
                }
            }
        ],
        "table_specifications" : [
            {
                "_id": "t_users",
                "type":"foaf:Person",
                "from":"CBD_user",
                "to_data_source" : "cluster2",
                "ensureIndexes":[
                    {
                        "value.name": 1
                    }
                ],
                "fields": [
                    {
                        "fieldName": "type",
                        "predicates": ["rdf:type"]
                    },
                    {
                        "fieldName": "name",
                        "predicates": ["foaf:name"]
                    },
                    {
                        "fieldName": "knows",
                        "predicates": ["foaf:knows"]
                    }
                ],
                "joins" : {
                    "foaf:knows" : {
                        "fields": [
                            {
                                "fieldName":"knows_name",
                                "predicates":["foaf:name"]
                            }
                        ]
                    }
                }
            }
        ],
        "search_config":{
            "search_provider":"MongoSearchProvider",
            "search_specifications":[
                {
                    "_id":"i_users",
                    "type":["foaf:Person"],
                    "from":"CBD_user",
                    "to_data_source" : "cluster2",
                    "filter":[
                        {
                            "condition":{
                                "foaf:name.l":{
                                    "$exists":true
                                }
                            }
                        }
                    ],
                    "indices":[
                        {
                            "fieldName": "name",
                            "predicates": ["foaf:name", "foaf:firstName","foaf:surname"]
                        }
                    ],
                    "fields":[
                        {
                            "fieldName":"result.name",
                            "predicates":["foaf:name"],
                            "limit" : 1
                        }
                    ]
                }
            ]
        },
    },
    "transaction_log" : {
        "database" : "testing",
        "collection" : "transaction_log",
        "data_source" : "cluster2"
    },
    "queue" : {
        "database" : "testing",
        "collection" : "q_queue",
        "data_source" : "cluster1"
    }
}

```

Internal data model
----

Data is stored in Mongo collections, one CBD per document. Typically you would choose to put all the data of a given object type in a distinct collection prefixed with ```CBD_```, e.g. ```CBD_users``` although this is more convention than requirement. 

These CBD collections are considered read and write from your application, and are subject to transactions recorded in the tlog (see Transactions below).

A CBD might look like this:

```javascript
{
	"_id" : {
		"r" : "http://example.com/user/2",
		"c" : "http://example.com/defaultContext"
	},
	"siocAccess:Role" : {
		"l" : "an undergraduate"
	},
	"siocAccess:has_status" : {
		"l" : "public"
	},
	"spec:email" : {
		"l" : "me@example.com"
	},
	"rdf:type" : [
		{
			"u" : "foaf:Person"
		},
		{
			"u" : "sioc:User"
		}
	],
	"foaf:name" : {
		"l" : "John Smith"
	}
}
```

A brief guide:

* Anything prefixed by underscore is a special field and managed exclusively by tripod. Here, ```_id``` is the composite of the subject (```r``` property for resource) and the named graph (```c``` property for context) for this CBD.
* Predicates are properties and are always namespaced, e.g. ```foaf:name```
* The value of the predicate fields are either an object or an array of objects (for multivalues). The object has exactly one property, either ```u``` (for uri, these are RDF resource object values) or ```l``` (for literal, these are RDF literal object values)

Transactions
----

MongoDB is only atomic at the document level. Tripod datasets store one [CBD](http://www.w3.org/Submission/CBD/) per document. Therefore an update to a graph of data can impact 1..n documents.

Tripod maintains a transaction log (tlog) of updates to allow rollback in the case of multi-document writes. It is possible (and recommended) to run this on a separate cluster to your main data. For disaster recovery, You can use the tlog to replay transactions on top of a known-good backup.

In production we run a small 2nd cluster in EC2 which stores up to 7 days of tlog, we prune and flush this periodically to S3. 

What have you built with this?
----

The majority of the datasets underpinning [Talis Aspire](http://www.talis.com), an enterprise SaaS course management system serving 1M students in over 50 universities worldwide, are powered using graph data stored in MongoDB via the Tripod library.

We built tripod when we needed to migrate away from our own in-house proprietary triple store (incidentally built around early versions of [Apache JENA](https://jena.apache.org/)).

We've been using it for 2 years in production. Our data volume is > 500M triples over 70 databases on modest 3-node clusters (2 data nodes) with Dell R710 mid-range servers, 12 cores 96Gb RAM, RAID-10 array of non-SSD disks, m1.small arbiter in EC2.


Why would I use this?
----

* Your query volumes are high but the number of distinct graph-traversal type queries required by your application is low (you'd be surprised how few distinct queries an application might need)
* The balance of your read/writes are in the order of >10:1
* Your datasets are probably in the range of no more than 100MT each (we've not tested beyond this)

When shouldn't I use this?
----

* You don't know the shape of your data
* You need to run ad-hoc graph traversal queries
* You need SPARQL support

Some further limitations
----

* Ad-hoc complex queries are a no as materialised views and tables are pre-specified. Changing these specifications requires that you re-generate materialised documents in their entirety.
* Relies heavy on namespaces. Inside the database predicates are always namespaced, infact it is a requirement to know and specify all your namespaces upfront in config, so working with arbitrary unknown data is not a strong point
* Writes are expensive because they trigger invalidation of views and tables. The more view and table specifications you have, the slower writes are (or the slower data in views and tables becomes consistent, if you are backgrounding their generation)

Coming soon (aka a loose roadmap)
----

* More docs
* ~~Node version - probably read-only subset in the first instance~~ It's [here](https://github.com/talis/tripod-node).
* Improvements to the background queue, currently this is a long running php script working from a queue of updates held in mongo. Only ever intended for the PoC but it's still here 2 years later!
* An alternative persistence technology for the tlog. Memory mapped databases are not good for datasets with rapid turnover as the data files grow even if the data set is pruned. Implement a more specialist append-only database or even a RDBMS for the tlog persistence. Being worked on: [issue](https://github.com/talis/tripod-php/issues/7), [branch](https://github.com/talis/tripod-php/tree/pgsql-tlog) and [PR](https://github.com/talis/tripod-php/pull/6)
* PHP >5.3.0 only. We still have some legacy servers on PHP 5.2 which is the only reason we continue support.
* Performance improvements for ```ExtendedGraph```. The internal structure of this object is a relic from the days of Talis' own proprietary triple store and how it used to return data. We bootstrap onto that using the ```MongoGraph``` object to marshal data in and out. This relies heavily on regex and we know that from our own data gathered in the field this is a single point of optimisation that would cut CPU cycles and memory usage. On the bright side it's nice to have such targeted, low hanging fruit to pick.
* Versioned config. This will allow views and tables to self-heal when their specs are updated. At present you have to delete and re-gen the lot when specs change.
* Publish updates to a spout for [Apache Storm](http://storm.incubator.apache.org/)
* Push the config inside the database, and version it. This will mean that multiple apps can work from the same database with the same config.
* Ability to specify specially annotated Entities in the fashion of Doctrine et al. These would be a way to implement models without the complexity of having to worry about `ExtendedGraph`. A lot of the config could be derived from the annotations on the Entity classes themselves. We are now working on an alpha of this, see [this issue](https://github.com/talis/tripod-php/issues/10). It will likely be an additional library that depends on Tripod rather than being merged into the core.
* We've learnt a lot since starting this project two years ago. Now the library is functionally stable we could do with re-architecting the object structure and responsibilities. That's one of the reasons we've open sourced - hopefully we'll be embarrassed into having a long over due tidy up.

Presentations
----

[We presented](http://www.slideshare.net/robotrobot/mongodb-uk) on an earlier version at MongoUK 2012. Since that time we have resolved the following todos: 

* Started the [node-js port](https://github.com/talis/tripod-node)
* Eliminated dependancy on map-reduce
* Added named graph support (context)
* Optimised the data model
* Open sourced the code
 
Credits
----

We make use of the excellent [ARC](https://github.com/semsol/arc2) and elements of Tripod are based on the [Moriarty](https://code.google.com/p/moriarty/) library, the fruit of some earlier work by Talis to provide a PHP library for Talis' own proprietary cloud triple store (no longer in operation).

The brainchild of [kiyanwang](https://github.com/kiyanwang) and [robotrobot](https://github.com/robotrobot) @ Talis

