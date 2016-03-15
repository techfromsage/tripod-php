Tripod views primer
====================

View specifications define the shape of the materialised views that Mongo manages.
In short, they mimic the functionality of DESCRIBE or CONSTRUCT-style SPARQL queries.

Viewspecs
----------

TODO: see [the config documentation page](../config.md) for a brief intro.

Specs are defined as an array in the store level of the config document:

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1"
                },
                {
                    "_id": "v_spec_2"
                }
            ]
        }
    }
}
```

Include
-------

The include keyword can be used to include only specific properties in the view.

For example, the following view would only include rdf:type and foaf:name properties:

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1",
                    "from": "CBD_something",
                    "type": "owl:Thing"
                    "include": ["rdf:type", "foaf:name"]
                }
            ]
        }
    }
}
```



Including an rdf:Sequence
-------------------------

It's not possible to include an entire rdf:Sequence using the standard include keyword. To use include, you have to explicitly name
each property to be included, but an rdf:Sequence has an unknown number of properties with numbered predicates. To include an entire
rdf:Sequence, use the "_seq_" keyword as shown in the example below.

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1",
                    "from": "CBD_something",
                    "type": "owl:Thing"
                    "include": ["_seq_"]
                }
            ]
        }
    }
}
```

Joins
-----

It is possible to materialise a view including a join to other subjects, either in the same pod or in a different pod.
The following is an example which creates a view joing owl:Things with anything they are a version of.

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1",
                    "from": "CBD_something",
                    "type": "owl:Thing"
                    "joins": {
                        "dct:isVersionOf": {
                                "include": ["dct:subject", "rdf:type"]
                        }
                    }
                }
            ]
        }
    }
}
```

Filtered Joins
--------------

It is possible that you may want to filter your view to only include specific subjects. Subjects included in the view can
be filtered to only include those that match any predicate. The following builds on the example from join above, but the view
will only include subjects which are a version of something with a type of owl:Other.

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1",
                    "from": "CBD_something",
                    "type": "owl:Thing"
                    "joins": {
                        "dct:isVersionOf": {
                            "filter" : {
                                "rdf:type" : {"u" : "owl:Other"}
                            },
                            "include": ["dct:subject", "rdf:type"]
                        }
                    }
                }
            ]
        }
    }
}
```

Here is another example, using a literal value for the filter. The view will only include subjects which
are a version of something which has a name John.

```javascript
{
    "stores": {
        "some_store": {
            "pods" : {},
            "view_specifications": [
                {
                    "_id": "v_spec_1",
                    "from": "CBD_something",
                    "type": "owl:Thing"
                    "joins": {
                        "dct:isVersionOf": {
                            "filter" : {
                                "foaf:name" : {"l" : "John"}
                            },
                            "include": ["dct:subject", "rdf:type"]
                        }
                    }
                }
            ]
        }
    }
}
```
