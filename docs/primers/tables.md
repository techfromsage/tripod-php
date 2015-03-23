Tripod tables primer
====================

Table specifications define the shape of the tabular data that Mongo manages.  They are analogous to SPARQL ``` SELECT ```
queries.

Tablespecs
----------

TODO: see [the config documentation page](../config.md) for a brief intro.

Tablespecs define the data that actually go into the resulting table row document.  They allow you to join across
pods for property values in other graphs, as well as generate values based on counts and computed values based on
other values in the data.

There are a few required properties:

```javascript
{
    "_id" : "t_some_table_name" // An identifier for the table type
    "from" : "CBD_something" // The CBD pod to begin generating the data from
    "type" : ["owl:Thing", "skos:Concept"] // the RDF type(s) to use as the initial filter to generate rows
}
```

From here, the tablespec have at least one of the following properties: "fields", "counts", "joins", or "computed_fields".

The value of all of these are are arrays of objects, except for "joins", whose value is an object.  Some examples:

 ```javascript
 {
     "_id" : "t_some_table_name"
     "from" : "CBD_something"
     "type" : ["owl:Thing", "skos:Concept"]
     "fields" : [
        {
            "fieldName": "title", // 'fieldName' is required in any field definition
            "predicates": ["dct:title"] // Every field must have a 'predicates' or 'value' property
        },
        {
            "fieldName": "resourceLink",
            "value": "link" // This turns the subject's ID into a fully qualified uri
        },
        {
            "fieldName": "!typeQname", // fieldNames preceded with '!' are temporary and will not be stored in the document
            "predicates": ["rdf:type"]
     ],
     "counts": [
        {
            "fieldName": "partCount",
            "property": "dct:hasPart" // will count how many time this property appears in this resource
        }
     ],
     "joins": {
        "dct:creator" : { // More or less the same as the top level of the tablespec
            "from": "CBD_authors", // Optional, allows you to specify another pod to join on. If left blank, will use the same pod
            "fields":
            {
                "fieldName": "authors",
                "predicates": [
                    { // There are modifiers that can be applied to the graph value before they're applied to the table row
                        "join": {
                            "glue": " ",
                            "predicates": ["foaf:firstName", "foaf:surname"]
                        }
                    } // examples: join [combine multiple values into a string], lowercase, date [turns a string date into a MongoDate]
                ]
            }
        }
     },
     "computed_fields": [ // this can only be at the top level of the tablespec, as it's applied after all other values are retrieved
        {
            "fieldName": "type",
            "value": {
                "_replace_" : { // _replace_ is a function name
                    "search" : "bibo:"
                    "replace" : "",
                    "subject" : "!typeQname" // will turn 'bibo:Book' into 'Book'
                }
            }
        }
     ]
 }
 ```


Usage
-----

```php

$tripod = MongoTripod("CBD_resources", "resources", array("async"=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));

// Here is the id the tableSpec
$tableSpec = 't_resources';

// This would return all table rows
$tripod->getTableRows($tableSpec);

=> array('head'=>array(
    'count'=>500,
    'offset'=>0,
    'limit'=>10
  ),
  'results'=>array(
        array(
            '_id' => array(
                'r'=>'foo:1234',
                'c'=>'http://example.com/
            ),
            'foo' => 'bar',
            'baz' => 'foo:wibble'
        ),
        array(
            '_id' => array(
                'r'=>'foo:5678',
                'c'=>'http://example.com/
            ),
            'foo' => 'something else',
            'baz' => 'foo:coelacanth'
        ),
        ...
  )
)

// You can filter the results with Mongo queries: note that the table data is actually stored in a 'value' object

$tripod->getTableRows($tableSpec, array('value.baz'=>'foo:coelacanth'));

=> array('head'=>array(
    'count'=>1,
    'offset'=>0,
    'limit'=>10
  ),
  'results'=>array(
        array(
            '_id' => array(
                'r'=>'foo:5678',
                'c'=>'http://example.com/'
            ),
            'foo' => 'something else',
            'baz' => 'foo:coelacanth'
        )
  )
)

// You can also get back the distinct values of a table column

$tripod->getDistinctTableValues($tableSpec, 'value.foo');

=> array('head'=>array(
    'count'=>2
  ),
  'results'=>array(
    'bar',
    'something else'
  )
)

// Or a filtered subset

$tripod->getDistinctTableValues($tableSpec, 'value.foo', array('value.baz'=>'foo:wibble'));

=> array('head'=>array(
    'count'=>1
  ),
  'results'=>array(
    'bar'
  )
)

```