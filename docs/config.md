Tripod config documentation
===========================

Tripod config is typically defined as a JSON file or stream which is added to the MongoTripodConfig class somewhere early in your application, typically in your includes file or front controller:

```php
$conf = json_decode(file_get_contents('tripod_config.json');
MongoTripodConfig::setConfig($conf); // set the config, usually read in as JSON from a file
```

Namespaces
---------

RDF namespaces are defined by a top level property `namespaces` which defines a simple object, the keys of which are the prefix, the value of which are the namespace.

Example:

```javascript
{
  "namespaces" : {
    "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dct":"http://purl.org/dc/terms/"
  }
}
```

Tripod relies on namespaces for subjects so any subject URIs must have a pre-declared namespace in the config.

TODO: Future versions will detect non-namespaced subjects and assign a namespace to it in config. For this to happen we must start storing the config in the database rather than as an external file.

Default context
---------

Tripod supports named graphs. The default context property defines the default named graph to use if one is not specified.

Example:

```javascript
{
  "defaultContext" : "http://example.com"
}
```

Databases
-------

Defines the databases and names the collections Tripod can work with. Also includes the ability to define indexes and OWL-like cardinality rules on predicates within each collection. Each database also defines a connection string telling Tripod how to connect.

Example:

This example defines one database with two `CBD_` collections along with associated indexes and cardinality rules.

```javascript
{
  "databases" : {
    "my_app_db" : {
      "collections" : {
        "CBD_orders" : {
          "cardinality" : {
            "dct:created" : 1
          },
          "indexes" : {
            "index1": {
              "dct:subject.u":1
            },
            "index2" : {
              "rdf:type.u":1
            }
          }
        },
        "CBD_users" : {
          "cardinality" : {
            "foaf:name.l" : 1
          },
          "indexes" : {
            "index1": {
              "rdf:type.u":1
            }
          }
        }
      },
      "connStr" : "mongodb://localhost"
    }
  }
}
```

View specifications
---------------

View specifications define the shape of the materialised views that Mongo manages. For a full explanation of views, [read the primer](primers/views.md). In short, they mimic the functionality of `DESCRIBE` or `CONSTRUCT`-style SPARQL queries.

The convention for view spec identifiers is to prefix them with `v_`.

Specs are defined as an array at the top level of the config document:

```javascript
{
  "view_specifications": [
    {
      "_id": "v_spec_1"
    },
    {
      "_id": "v_spec_2"
    }
  ]
}
```

TODO: 

* Implement versioned view specifications to allow automatic migration of data that meets an earlier specification

Table specifications
---------------

Table specifications define the shape of the tabular data that Mongo manages. For a full explanation of tables, [read the primer](primers/tables.md). In short, they mimic the functionality of `SELECT`-style SPARQL queries.

The convention for table spec identifiers is to prefix them with `t_`.

Specs are defined as an array at the top level of the config document:

```javascript
{
  "table_specifications": [
    {
      "_id": "t_spec_1"
    },
    {
      "_id": "t_spec_2"
    }
  ]
}
```

TODO: 

* Implement versioned table specifications to allow automatic migration of data that meets an earlier specification


Search config
---------------

Previous versions of Tripod integrated with ElasticSearch to provide indexing and full-text search. This was removed early on whilst Tripod was still closed source within Talis, as the complexity was not required. However some primitive regex-style searching is still provided.  For a full explanation of views, [read the primer](primers/search.md).

The search config is defined at the top level and consists of two parts - the `search_provider` and `search_specifications`.

The provider was intended to allow pluggable implementations of search services (ElasticSearch, straight Lucene, Solr perhaps) but today the only option is `MongoSearchProvider`.

The search specifications define the shape of the the data that underpins searches.

The convention for search spec identifiers is to prefix them with `i_`.

```javascript
{
  "search_config": {
    "search_provider" : "MongoSearchProvider",
    "search_specifications" :   [
      {
        "_id": "i_spec_1"
      },
      {
        "_id": "i_spec_2"
      }
    ]
  }
}
```

TODO:

* Clean up the search specifications as they are not quite in line with tables and views (notably filter/condition)
* At some point re-instate full-text capability via ElasticSearch or similar.


Specification keyword reference
---------------

Each of the specifications above are built from a specification language defined by the keywords below

### _id

### _version

### from

### type

### include

### joins 

### maxJoins

### predicates

### limit

### condition

### counts

#### counts / property

#### counts / regex

### filter

#### filter/condition

### ttl

### indicies

### ensureIndexes

### lowercase

### glue

### value

`"value" : "_link_"`
