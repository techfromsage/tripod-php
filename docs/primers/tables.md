Tripod tables primer
====================

Table specifications define the shape of the tabular data that Mongo manages.  They are analogous to SPARQL ``` SELECT ```
queries.

Tablespecs
----------

TODO: see [the config documentation page](../config.md) for a brief intro.

Usage
-----

```php

$tripod = MongoTripod("CBD_resources", "resources", array("async"=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));

// Here is the id the tableSpec
$tableSpec = 't_resources';

// This would return all table rows
$tripod->getTripodTables()->getTableRows($tableSpec);

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

$tripod->getTripodTables()->getTableRows($tableSpec, array('value.baz'=>'foo:coelacanth'));

=> array('head'=>array(
    'count'=>1,
    'offset'=>0,
    'limit'=>10
  ),
  'results'=>array(
        array(
            '_id' => array(
                'r'=>'foo:5678',
                'c'=>'http://example.com/
            ),
            'foo' => 'something else',
            'baz' => 'foo:coelacanth'
        )
  )
)

// You can also get back the distinct values of a table column

$tripod->getTripodTables()->distinct($tableSpec, 'values.foo');

=> array('head'=>array(
    'count'=>2
  ),
  'results'=>array(
    'bar',
    'something else'
  )
)

// Or a filtered subset

$tripod->getTripodTables()->distinct($tableSpec, 'values.foo', array('value.baz'=>'foo:wibble'));

=> array('head'=>array(
    'count'=>1
  ),
  'results'=>array(
    'bar'
  )
)

```