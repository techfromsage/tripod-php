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
            "fieldName": "typeQname",
            "predicates": ["rdf:type"],
            "temporary": true  // fieldNames with the temporary property set to true will not be stored in the document
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
                "replace" : { // replace is a function name
                    "search" : "bibo:"
                    "replace" : "",
                    "subject" : "typeQname" // will turn 'bibo:Book' into 'Book'
                }
            }
        }
     ]
 }
 ```

Temporary fields
----------------
Any table field can be defined as temporary, whether it is defined in 'fields', 'counts', or 'computed_fields'.  Temporary
fields will not be stored in the table row document.  Note: if a fieldName is designated as temporary *anywhere* within the
table specification, it will apply to all references to that fieldName, e.g.

```javascript
{
     "_id" : "t_some_table_name"
     "from" : "CBD_something"
     "type" : ["owl:Thing", "skos:Concept"]
     "fields" : [
        {
            "fieldName": "foo",
            "predicates": ["skos:label"],
            "temporary": true
        }
     ],
     "joins": {
        "owl:sameAs" : {
            "fields" : [
                { // This field will not be saved
                    "fieldName": "foo",
                    "predicates": ["skos:label"]
                }
            ]
        }
     }
}
```

Computed fields
---------------
Computed fields allow you to set a table row value based on functions run against other fields in the table row document.
These are run *after* all of the CBD-based values ('fields', 'joins', and 'counts') has been gathered, and the 'computed_fields'
property can *only* exist at the base level of the table specification.

Computed fields *must* contain 'fieldName' and 'value' properties and 'value' must be a valid computed field function
 specification.  Optionally, they may also contain a 'property' property.

Computed fields are run in the order they appear in the specification, so a later fields can use the results of earlier functions.

Functions can be nested, so the input values of a function can be supplied by another function, e.g.:

```javascript
{
    ...
    "computed_fields": [
        {
            "fieldName": "foo",
            "value": {
                "conditional" : // This defines the function to run
                {
                    "if:[
                        {
                            "arithmetic": // This will run an arithmetic function to set the left side of the condition
                            {
                                ["$x", "*", 10]
                            }
                        }, ">", 100
                    ],
                    "then": "foo",
                    "else": "bar"
                }
            }

        }
    ]
}
```

Only *one* function can be defined at a time, however.  This is *not* valid:

```javascript
{
    ...
    "computed_fields": [
        {
            "fieldName": "foo",
            "value": {
                "replace": {
                    "search": "foo",
                    "replace": "bar",
                    "subject": "$x"
                },
                "arithmetic":["$x", "-", 4"]
            }
        }
    ]
}
```

Fields are referenced as variables defined by the fieldName prefixed with ``` $ ```: ``` "fieldName": "foo" ``` would be ``` $foo ```, etc.

Conditional functions
---------------------

Conditionals let you set the value of the table field based on other values.  Conditionals *must* contain an 'if' property
and must have either a 'then' or an 'else' property, or both.

"if" expects an array with a length of 1 or 3 as its value: if the array's count is 1, it will just check for the existence
of the value:

```javascript
{
    "fieldName": "foo",
    "value": {
        "conditional": {
            "if": ["$x"] // Does the fieldName 'x' have a value?
            "then": true,
            "else": false
        }
    }
}
```

If the array's count is three, the 2nd value must be one of:
">","<",">=", "<=", "==", "!=", "contains", "not contains", "~=", "!~"
there is no strict type comparison.

"then" and "else" values may be of any type, including another function.  To replicate 'else if', you can use another
 conditional function as the value of 'else'.

 Arithmetic functions
 --------------------
 Arithmetic functions allow you to do simple arithmetic with the values.  The function will try to cast the value to the
 most appropriate type (first it will try float, then integer).

 The arithmetic function's value is an array with 3 values: the first and last values can be arrays to allow for order of operations.

 ```javascript
 {
    "fieldName": "foo",
    "value": {
        "arithmetic": [["$a", "+", "$b"], "*", ["$x", "+", "$y"]]
    }
 }
```

The left and right values of the equation can also be nested functions:

 ```javascript
 {
    "fieldName": "foo",
    "value": {
        "arithmetic": [
            [{
                "conditional": {
                    "if":["$a" ">" "$c"],
                    "then": "$a",
                    "else": "$c"
                }
            }, "+", "$b"], "*", ["$x", "+", "$y"]
        ]
    }
 }
```

The second value must be one of:
"+", "-", "*", "/", "%"

Replace functions
-----------------

This function allows you to do a string replace.  Its syntax is analogous to PHP's ``` str_replace ``` function.

It *must* have 'search', 'replace', and 'subject' properties.  Like ``` str_replace ```, 'search' and 'replace' can be
   arrays.

If, for this example, ``` $x ``` is "bibo:Article"

```javascript
{
    "fieldName": "foo",
    "value": {
        "replace": {
            "search": "bibo:",
            "replace": "",
            "subject": "$x"
        }
     }
}
```

This would set 'foo' to 'Article'.

Usage
-----

```php

$tripod = new Driver("CBD_resources", "resources", array("async"=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));

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