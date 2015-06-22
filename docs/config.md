Tripod config documentation
===========================

Tripod config is typically defined as a JSON file or stream which is added to the Config class somewhere early in your application, typically in your includes file or front controller:

```php
$conf = json_decode(file_get_contents('tripod_config.json');
\Tripod\Mongo\Config::setConfig($conf); // set the config, usually read in as JSON from a file
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

The unique identifier of the spec

### _version

Specifies the version of the spec. Unused until we implement version specification.

### from

For the current operation specifies the collection the operation should be performed on. Within `joins` this allows you to join data across collections. It is mandatory at the top level of a specification and gives the starting collection from where the specification should operate. For example, to join from one collection to another:

```javascript
{
  "_id": "v_someview",
  "from": "CBD_mydata
  "joins" : {
    "foaf:knows": {
      "from":"CBD_myotherdata"
    }
  }
}
```

### type

If _type_ is defined, will limit the resources to those that have the specified rdf:type.  The value can be a curie
string or array of curie strings. For example:

```javascript
{
    "_id" : "v_people",
    "type" : ["foaf:Agent", "foaf:Person"],
    "from" : "CBD_people"
}

{
    "_id" : "t_books",
    "type" : "bibo:Books",
    "from" : "CBD_resources"
}

```
etc.

### include

A property of the `joins` predicate object, is an array of predicate values to pluck from the joined CBD and include in the result. If ommitted, all values from the CBD will be included. Allows you to mimic the behaviour of a `CONSTRUCT` style SPARQL query by slimming down the resultant graph only to predicates you specify.

### joins

joins the current resource to another.  The keys of the "joins" object correspond with the predicate whose object URI you
 wish to join on.  The "right join" will be on the \_id property in the joined resource.  You can specify the collection
 to join on with the "from" property (defaults to the current collection).

Note: you can *only* join a URI object (or \_id) to an \_id.

 Example:
```javascript
{
    "_id" : "t_people",
    "type" : "foaf:Person",
    "from" : "CBD_people",
    "fields" : [
        {
            "fieldName" : "name",
            "predicates" : ["foaf:name"]
        }
    ],
    "joins" : {
        "foaf:knows" : [
            {
                "fieldName" : "knows",
                "predicates" : ["foaf:name"]
            }
        ]

    }
}

```

### maxJoins

A property of the `joins` predicate object, determines the maximum amount of times a join will be performed. Where the amount of available matches exceeds `maxJoins` there are no guarantees on which of the available matches will be included. The exception to this is when used with a sequence via `followSequence`, here sequences `rdf:_1..rdf:_{maxJoin}` will be included.

### followSequence

[RDF sequences](http://www.w3.org/TR/rdf-schema/#ch_seq) are triple structures with a type of `rdf:Seq` and enumerate entities with the predicates `rdf:_x`, e.g. `rdf:_1, rdf:_2` etc. These have always been tricky to work with in SPARQL queries.

In tripod, when joining to a node which is actually a sequence, you would have to manually join again from the sequence to each element `rdf:_1` etc. which is hard because you'll need to know the length of the sequence up-front, and view specs are not dynamic (they are specified in config, not at runtime).

`followSequence` simplifies this by providing a shortcut for following sequences and joins automatically until either the last sequence element is reached, or `maxJoins` is exceeded. For example:

```javascript
  "bibo:authorList":{
    "joins" : {
      "followSequence":{
        "maxJoins":50
      }
    }
}
```

The properties of the `followSequence` object are identical in behaviour to those that can be specified in the `joins` predicate object.

### predicates

An array of predicates to use in the current action.  There are a few functions that work with "predicates", as well,
that do post processing on the results, such as "lowercase" and "join".

### limit

### condition

A property of the `join` object, specifies a query condition which must be matched for the join to be performed.

### counts

### counts / property

### counts / regex

### filter

### filter/condition

### ttl

`ttl` can be used in view specifications at the top level to indicate the time of expiry of the data. Views generated with a `ttl` will not have an impact index, that is, write operations will not automatically expire them. Instead, when they are read, tripod will look at the timestamp of the view's creation and if it exceeds the ttl it will discard the view and regenerate (and store) a new one and return that instead.

This is very useful if you have specific volatile views and the freshest data is not always cruicial - you can avoid excessive view re-generation by specifying a `ttl` value which exceeds the mean time between writes to your data.

`ttl` cannot be used within table specifications, because tablerows are often operated on in paged sets. It would be impossible to tell if table rows on further counts should still exist without paging through the whole set first.

### indicies

### ensureIndexes

### lowercase

### glue

### value

"value" defines a function to run on the property data.

### "value" : "link"

Creates a fully qualified URI from the alias value of the _current_ resource.  E.g.:

```javascript
{
    "id" : "t_foo",
    "from" : "fooCollection",
    "fields" : [
       {
            "fieldName" : "fooLink",
            "predicates" : [""],
            "value" : "link"
       }
    ]
}
```

would give the fully qualified URI of the base resource in field ``` fooLink ``` .  In a join:

```javascript
{
    "id" : "t_foo",
    "from" : "fooCollection",
    "joins" : {
        "foo:bar" :  {
            "from" : "barCollection",
            "fields" : [
               {
                    "fieldName" : "barLink",
                    "predicates" : [""],
                    "value" : "link"
               }]
        }
    }
}
```
\_link\_ would provide the fully qualified URI of the resource joined at ``` foo:bar ``` in the field ``` barLink ```

the "predicates" property is required, but ignored, so use an array with a single empty string: [""]

