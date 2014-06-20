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

Table specifications
---------------

Search specifications
---------------


Specification keyword reference
---------------

### from

### type

### include

### joins 

### maxJoins

### counts

#### counts / property

#### counts / filter

#### counts / regex

### ensureIndexes

### lowercase

### glue

### value

`"value" : "_link_"`
