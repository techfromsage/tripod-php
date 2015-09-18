<?php

namespace Tripod\Mongo\Composites;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR . 'mongo/base/DriverBase.class.php';

use Tripod\Mongo\Config;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Labeller;

/**
 * Class Tables
 * @package Tripod\Mongo\Composites
 */
class Tables extends CompositeBase
{
    /**
     * Modifier config - list of allowed functions and their attributes that can be passed through in tablespecs.json
     * Note about the "true" value - this is so that the keys are defined as keys rather than values. If we move to
     * a json schema we could define the types of attribute and whether they are required or not
     * @var array
     * @static
     */
    public static $predicateModifiers = array(
        'join' => array(
            'glue' => true,
            'predicates' => true
        ),
        'lowercase' => array(
            'predicates' => true
        ),
        'date' => array(
            'predicates' => true
        )
    );

    /**
     * Computed field config - A list of valid functions to write dynamic table row field values
     *
     * @var array
     * @static
     */
    public static $computedFieldFunctions = array('conditional', "replace", "arithmetic");

    /**
     * Computed conditional config - list of allowed conditional operators
     *
     * @var array
     * @static
     */
    public static $conditionalOperators = array(">","<",">=", "<=", "==", "!=", "contains", "not contains", "~=", "!~");

    /**
     * Computed arithmetic config - list of allowed arithmetic operators
     *
     * @var array
     * @static
     */
    public static $arithmeticOperators = array("+", "-", "*", "/", "%");

    /**
     * @var array
     */
    protected $temporaryFields = array();

    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there
     * @param string $storeName
     * @param \MongoCollection $collection
     * @param string $defaultContext
     * @param \Tripod\ITripodStat|null $stat
     * @param string $readPreference
     * todo: MongoCollection -> podName
     */
    public function __construct($storeName,\MongoCollection $collection,$defaultContext,$stat=null,$readPreference=\MongoClient::RP_PRIMARY)
    {
        $this->labeller = new Labeller();
        $this->storeName = $storeName;
        $this->collection = $collection;
        $this->podName = $collection->getName();
        $this->config = Config::getInstance();
        $this->defaultContext = $this->labeller->uri_to_alias($defaultContext); // make sure default context is qnamed if applicable
        $this->stat = $stat;
        $this->readPreference = $readPreference;
    }

    /**
     * Receive update from subject
     * @param \Tripod\Mongo\ImpactedSubject
     * @return void
     */
    public function update(ImpactedSubject $subject)
    {
        $resource = $subject->getResourceId();
        $resourceUri    = $resource[_ID_RESOURCE];
        $context        = $resource[_ID_CONTEXT];

        $this->generateTableRowsForResource($resourceUri,$context,$subject->getSpecTypes());
    }

    /**
     * Returns an array of the rdf types that will trigger the table specification
     * @return array
     */
    public function getTypesInSpecifications()
    {
        return $this->config->getTypesInTableSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * Returns an array of table rows that are impacted by the changes
     * @param array $resourcesAndPredicates
     * @param string $contextAlias
     * @return array
     */
    public function findImpactedComposites(Array $resourcesAndPredicates, $contextAlias)
    {
        $contextAlias = $this->getContextAlias($contextAlias); // belt and braces

        $tablePredicates = array();

        foreach(Config::getInstance()->getTableSpecifications($this->storeName) as $tableSpec)
        {
            if(isset($tableSpec[_ID_KEY]))
            {
                $tablePredicates[$tableSpec[_ID_KEY]] = Config::getInstance()->getDefinedPredicatesInSpec($this->storeName, $tableSpec[_ID_KEY]);
            }
        }

        // build a filter - will be used for impactIndex detection and finding direct tables to re-gen
        $tableFilters = array();
        $resourceFilters = array();
        foreach ($resourcesAndPredicates as $resource=>$resourcePredicates)
        {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $id = array(_ID_RESOURCE=>$resourceAlias,_ID_CONTEXT=>$contextAlias);
            // If we don't have a working config or there are no predicates listed, remove all
            // rows associated with the resource in all tables
            if(empty($tablePredicates) || empty($resourcePredicates))
            {
                // build $filter for queries to impact index
                $resourceFilters[] = $id;
            }
            else
            {
                foreach($tablePredicates as $tableType=>$predicates)
                {
                    // Only look for table rows if the changed predicates are actually defined in the tablespec
                    if(array_intersect($resourcePredicates, $predicates))
                    {
                        if(!isset($tableFilters[$tableType]))
                        {
                            $tableFilters[$tableType] = array();
                        }
                        // build $filter for queries to impact index
                        $tableFilters[$tableType][] = $id;
                    }
                }
            }

        }

        if(empty($tableFilters) && !empty($resourceFilters))
        {
            $query = array("value."._IMPACT_INDEX=>array('$in'=>$resourceFilters));
        }
        else
        {
            $query = array();
            foreach($tableFilters as $tableType=>$filters)
            {
                // first re-gen table rows where resources appear in the impact index
                $query[] = array("value."._IMPACT_INDEX=>array('$in'=>$filters), '_id.'._ID_TYPE=>$tableType);
            }

            if(!empty($resourceFilters))
            {
                $query[] = array("value."._IMPACT_INDEX=>array('$in'=>$resourceFilters));
            }

            if(count($query) === 1)
            {
                $query = $query[0];
            }
            elseif(count($query) > 1)
            {
                $query = array('$or'=>$query);
            }
        }

        if(empty($query))
        {
            return array();
        }

        $affectedTableRows = array();

        foreach($this->config->getCollectionsForTables($this->storeName) as $collection)
        {
            $tableRows = $collection->find($query, array("_id"=>true));
            foreach($tableRows as $t)
            {
                $affectedTableRows[] = $t;
            }
        }

        return $affectedTableRows;
    }

    /**
     * @param string $storeName
     * @param string $tableSpecId
     * @return array|null
     */
    public function getSpecification($storeName, $tableSpecId)
    {
        return $this->config->getTableSpecification($storeName,$tableSpecId);
    }

    /**
     * Returns the operation this composite can satisfy
     * @return string
     */
    public function getOperationType()
    {
        return OP_TABLES;
    }

    /**
     * Query the tables collection and return the results
     * @param string $tableSpecId
     * @param array $filter
     * @param array $sortBy
     * @param int $offset
     * @param int $limit
     * @return array
     */
    public function getTableRows($tableSpecId,$filter=array(),$sortBy=array(),$offset=0,$limit=10)
    {
        $t = new \Tripod\Timer();
        $t->start();

        $filter["_id." . _ID_TYPE] = $tableSpecId;

        $collection = $this->config->getCollectionForTable($this->storeName, $tableSpecId, $this->readPreference);
        $results = (empty($limit)) ? $collection->find($filter) : $collection->find($filter)->skip($offset)->limit($limit);
        if (isset($sortBy))
        {
            $results->sort($sortBy);
        }
        $rows = array();
        foreach ($results as $doc)
        {
            if (array_key_exists(_IMPACT_INDEX,$doc['value'])) unset($doc['value'][_IMPACT_INDEX]); // remove impact index from client
            $rows[] = $doc['value'];
        }

        $t->stop();
        $this->timingLog(MONGO_TABLE_ROWS, array('duration'=>$t->result(), 'query'=>$filter, 'collection'=>TABLE_ROWS_COLLECTION));
        $this->getStat()->timer(MONGO_TABLE_ROWS.".$tableSpecId",$t->result());
        $this->getStat()->increment(MONGO_TABLE_ROWS.".$tableSpecId");

        return array(
            "head"=>array(
                "count"=>$results->count(),
                "offset"=>$offset,
                "limit"=>$limit
            ),
            "results"=>$rows);
    }

    /**
     * Returns the distinct values for a table column, optionally filtered by query
     *
     * @param string $tableSpecId
     * @param string $fieldName
     * @param array $filter
     * @return array
     */
    public function distinct($tableSpecId, $fieldName, array $filter=array())
    {
        $t = new \Tripod\Timer();
        $t->start();

        $filter['_id.'._ID_TYPE] = $tableSpecId;

        $collection = $this->config->getCollectionForTable($this->storeName, $tableSpecId, $this->readPreference);
        $results = $collection->distinct($fieldName, $filter);

        $t->stop();
        $query = array('distinct'=>$fieldName, 'filter'=>$filter);
        $this->timingLog(MONGO_TABLE_ROWS_DISTINCT, array('duration'=>$t->result(), 'query'=>$query, 'collection'=>TABLE_ROWS_COLLECTION));
        $this->getStat()->timer(MONGO_TABLE_ROWS_DISTINCT.".$tableSpecId",$t->result());
        return array(
            "head"=>array(
                "count"=>count($results)
            ),
            "results"=>$results
        );
    }

    /**
     * @param string $resource The URI or alias of the resource to delete from tables
     * @param string|null $context Optional context
     * @param array|string|null $specType Optional table type or array of table types to delete from
     */
    protected function deleteTableRowsForResource($resource, $context=null, $specType = null)
    {
        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);
        $query = array(_ID_KEY . '.' . _ID_RESOURCE => $resourceAlias,  _ID_KEY . '.' . _ID_CONTEXT => $contextAlias);
        $specNames = array();
        $specTypes = $this->config->getTableSpecifications($this->storeName);
        if (empty($specType)) {
            $specNames = array_keys($specTypes);
        }
        else
        {
            if(is_string($specType))
            {
                $query[_ID_KEY][_ID_TYPE] = $specType;
                $specNames = array($specType);
            }
            elseif(is_array($specType))
            {
                $query[_ID_KEY . '.' . _ID_TYPE] = array('$in'=>$specType);
                $specNames = $specType;
            }
        }
        foreach($specNames as $specName)
        {
            // Ignore any other types of specs that might have been passed in here
            if(isset($specTypes[$specName]))
            {
                $this->config->getCollectionForTable($this->storeName, $specName)->remove($query);
            }
        }
    }

    /**
     * This method will delete all table rows where the _id.type matches the specified $tableId
     * @param string $tableId
     */
    public function deleteTableRowsByTableId($tableId) {
        $tableSpec = Config::getInstance()->getTableSpecification($this->storeName, $tableId);
        if ($tableSpec==null)
        {
            $this->debugLog("Could not find a table specification for $tableId");
            return;
        }

        $this->config->getCollectionForTable($this->storeName, $tableId)
            ->remove(array("_id.type"=>$tableId), array('fsync'=>true));
    }

    /**
     * This method handles invalidation and regeneration of table rows based on impact index, before delegating to
     * generateTableRowsForType() for re-generation of any table rows for the $resource
     * @param string $resource
     * @param string|null $context
     * @param array $specTypes
     */
    protected function generateTableRowsForResource($resource, $context=null, $specTypes=array())
    {
        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $this->deleteTableRowsForResource($resource, $context, $specTypes);

        $filter = array();
        $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);

        // now go through the types
        $query = array("_id"=>array('$in'=>$filter));
        $resourceAndType = $this->config->getCollectionForCBD($this->storeName, $this->podName)
            ->find($query,array("_id"=>1,"rdf:type"=>1));

        foreach ($resourceAndType as $rt)
        {
            $id = $rt["_id"];
            if (array_key_exists("rdf:type",$rt))
            {
                if (array_key_exists('u',$rt["rdf:type"]))
                {
                    // single type, not an array of values
                    $this->generateTableRowsForType($rt["rdf:type"]['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT], $specTypes);
                }
                else
                {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type)
                    {
                        // Defensive check in case there is bad data for rdf:type
                        if(array_key_exists('u', $type))
                        {
                            $this->generateTableRowsForType($type['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT], $specTypes);
                        }
                    }
                }
            }
        }
    }

    /**
     * This method finds all the table specs for the given $rdfType and generates the table rows for the $subject one by one
     * @param string $rdfType
     * @param string|null $subject
     * @param string|null $context
     * @param array $specTypes
     * @return mixed
     */
    public function generateTableRowsForType($rdfType,$subject=null,$context=null, $specTypes = array())
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;

        if(empty($specTypes))
        {
            $tableSpecs = Config::getInstance()->getTableSpecifications($this->storeName);
        }
        else
        {
            $tableSpecs = array();
            foreach($specTypes as $specType)
            {
                $spec = Config::getInstance()->getTableSpecification($this->storeName, $specType);
                if($spec)
                {
                    $tableSpecs[$specType] = $spec;
                }
            }
        }

        foreach($tableSpecs as $key=>$tableSpec)
        {
            if(isset($tableSpec["type"]))
            {
                $types = $tableSpec["type"];
                if(!is_array($types))
                {
                    $types = array($types);
                }
                if (in_array($rdfType, $types) || in_array($rdfTypeAlias, $types))
                {
                    $foundSpec = true;
                    $this->debugLog("Processing {$tableSpec[_ID_KEY]}");
                    $this->generateTableRows($key,$subject,$context);
                }
            }
        }
        if (!$foundSpec)
        {
            $this->debugLog("Could not find any table specifications for $subject with resource type '$rdfType'");
            return;
        }
    }

    /**
     * @param string $tableType
     * @param string|null $resource
     * @param string|null $context
     * @param string|null $queueName Queue for background bulk generation
     * @return null //@todo: this should be a bool
     */
    public function generateTableRows($tableType,$resource=null,$context=null,$queueName=null)
    {
        $t = new \Tripod\Timer();
        $t->start();
        $this->temporaryFields = array();
        $tableSpec = Config::getInstance()->getTableSpecification($this->storeName, $tableType);
        $collection = $this->config->getCollectionForTable($this->storeName, $tableType);

        if ($tableSpec==null)
        {
            $this->debugLog("Could not find a table specification for $tableType");
            return null;
        }

        // ensure that the ID field, view type, and the impactIndex indexes are correctly set up
        $collection->ensureIndex(
            array(
                '_id.r'=>1,
                '_id.c'=>1,
                '_id.type'=>1
            ),
            array(
                'background'=>1
            )
        );

        $collection->ensureIndex(
            array(
                '_id.type'=>1
            ),
            array(
                'background'=>1
            )
        );

        $collection->ensureIndex(
            array(
                'value.'._IMPACT_INDEX=>1
            ),
            array(
                'background'=>1
            )
        );

        // ensure any custom view indexes
        foreach (Config::getInstance()->getTableSpecifications($this->storeName) as $tSpec)
        {
            if (isset($tSpec['ensureIndexes']) && $tSpec['to_data_source'] == $tableSpec['to_data_source']) // only ensure table_rows indexes for the data source that matches the table spec we're generating
            {
                foreach ($tSpec['ensureIndexes'] as $ensureIndex)
                {
                    $this->ensureIndex(
                        $collection,
                        $ensureIndex
                    );
                }
            }
        }

        // default the context
        $contextAlias = $this->getContextAlias($context);

        // default collection
        $from = (isset($tableSpec["from"])) ? $tableSpec["from"] : $this->podName;

        $types = array();
        if (is_array($tableSpec["type"]))
        {
            foreach ($tableSpec["type"] as $type)
            {
                $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($type));
                $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($type));
            }
        }
        else
        {
            $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($tableSpec["type"]));
            $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($tableSpec["type"]));
        }
        $filter = array('$or'=> $types);
        if (isset($resource))
        {
            $filter["_id"] = array(_ID_RESOURCE=>$this->labeller->uri_to_alias($resource),_ID_CONTEXT=>$contextAlias);
        }

        $docs = $this->config->getCollectionForCBD($this->storeName, $from)->find($filter);
        $docs->timeout(\Tripod\Mongo\Config::getInstance()->getMongoCursorTimeout());

        foreach ($docs as $doc)
        {
            if($queueName && !$resource)
            {
                $subject = new ImpactedSubject(
                    $doc['_id'],
                    OP_TABLES,
                    $this->storeName,
                    $from,
                    array($tableType)
                );

                $jobOptions = array();

                if($this->stat || !empty($this->statsConfig))
                {
                    $jobOptions['statsConfig'] = $this->getStatsConfig();
                }

                $this->getApplyOperation()->createJob(array($subject), $queueName, $jobOptions);
            }
            else
            {
                // set up ID
                $generatedRow = array("_id"=>array(_ID_RESOURCE=>$doc["_id"][_ID_RESOURCE],_ID_CONTEXT=>$doc["_id"][_ID_CONTEXT],_ID_TYPE=>$tableSpec['_id']));

                $value = array('_id'=>$doc['_id']); // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R
                $this->addIdToImpactIndex($doc['_id'], $value); // need to add the doc to the impact index to be consistent with views/search etc. this is needed for discovering impacted operations
                $this->addFields($doc,$tableSpec,$value);
                if (isset($tableSpec['joins']))
                {
                    $this->doJoins($doc,$tableSpec['joins'],$value,$from,$contextAlias);
                }
                if (isset($tableSpec['counts']))
                {
                    $this->doCounts($doc,$tableSpec['counts'],$value);
                }

                if (isset($tableSpec['computed_fields']))
                {
                    $this->doComputedFields($tableSpec, $value);
                }

                // Remove temp fields from document

                $generatedRow['value'] = array_diff_key($value, array_flip($this->temporaryFields));
                $this->truncatingSave($collection, $generatedRow);
            }
        }

        $t->stop();
        $this->timingLog(MONGO_CREATE_TABLE, array(
            'type'=>$tableSpec['type'],
            'duration'=>$t->result(),
            'filter'=>$filter,
            'from'=>$from));
        $this->getStat()->timer(MONGO_CREATE_TABLE.".$tableType",$t->result());
        $this->getStat()->increment(MONGO_CREATE_TABLE.".$tableType");
    }

    /**
     * Save the generated rows to the given collection.
     *
     * If an exception in thrown because a field is too large to index, the field is
     * truncated and the save is retried.
     *
     * @param \MongoCollection $collection
     * @param array $generatedRow The rows to save.
     * @throws \Exception
     */
    protected function truncatingSave(\MongoCollection $collection, array $generatedRow)
    {
        try
        {
            $collection->save($generatedRow);
        } catch (\Exception $e) {
            // We only truncate and retry the save if the \Exception contains this text.
            if (strpos($e->getMessage(),"Btree::insert: key too large to index") !== FALSE)
            {
                $this->truncateFields($collection, $generatedRow);
                $collection->save($generatedRow);
            }
            else
            {
                throw $e;
            }
        }
    }

    /**
     * Truncate any indexed fields in the generated rows which are too large to index
     *
     * @param \MongoCollection $collection
     * @param array $generatedRow - Pass by reference so that the contents is truncated
     */
    protected function truncateFields(\MongoCollection $collection, array &$generatedRow)
    {
        // Find the name of any indexed fields
        $indexedFields = array();
        $indexesGroupedByCollection = $this->config->getIndexesGroupedByCollection($this->storeName);
        if (isset($indexesGroupedByCollection) && isset($indexesGroupedByCollection[$collection->getName()]))
        {
            $indexes = $indexesGroupedByCollection[$collection->getName()];
            if (isset($indexes))
            {
                foreach($indexes as $repset)
                {
                    foreach ($repset as $index)
                    {
                        foreach ($index as $indexedFieldname => $v)
                        {
                            if (strpos($indexedFieldname, "value.") === 0)
                            {
                                $indexedFields[] = substr($indexedFieldname, strlen('value.'));
                            }
                        }
                    }
                }
            }
        }

        if (count($indexedFields) > 0 && isset($generatedRow['value']) && is_array($generatedRow['value']))
        {
            // Iterate over generated rows BY REFERENCE (&) - we are going to modify the contents of $field
            foreach($generatedRow as &$field)
            {
                foreach($indexedFields as $indexedFieldname)
                {
                    // The key will have the index name in the following format added to it.
                    // Adjust the max key size allowed to take it into account.
                    $maxKeySize = 1024 - strlen("value_".$indexedFieldname."_1");

                    if (array_key_exists($indexedFieldname, $field))
                    {
                        // It's important that we count the number of bytes
                        // in the field - not just the number of characters.
                        // UTF-8 characters can be between 1 and 4 bytes.
                        //
                        // From the strlen documentation:
                        //     Attention with utf8:
                        //     $foo = "bär";
                        //     strlen($foo) will return 4 and not 3 as expected..
                        //
                        // So strlen does count the bytes - not the characters.

                        if (is_string($field[$indexedFieldname]) && strlen($field[$indexedFieldname]) > $maxKeySize)
                        {
                            $field[$indexedFieldname] = substr($field[$indexedFieldname],0, $maxKeySize);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param array $spec The table spec
     * @param array $dest The table row document to save
     */
    protected function doComputedFields(array $spec, array &$dest)
    {
        if (isset($spec['computed_fields']))
        {
            foreach ($spec['computed_fields'] as $f)
            {
                if(isset($f['fieldName']) && isset($f['value']) && is_array($f['value']))
                {
                    if(isset($f['temporary']) && $f['temporary'] === true)
                    {
                        if(!in_array($f['fieldName'], $this->temporaryFields))
                        {
                            $this->temporaryFields[] = $f['fieldName'];
                        }
                    }
                    $computedFunctions = array_values(array_intersect(self::$computedFieldFunctions, array_keys($f['value'])));
                    $dest[$f['fieldName']] = $this->getComputedValue($computedFunctions[0], $f['value'], $dest);
                }
            }
        }
    }

    /**
     * @param string $function A defined computed value function
     * @param array $spec The computed field spec
     * @param array $dest The table row document to save
     * @return mixed The computed value
     */
    protected function getComputedValue($function, array $spec, array &$dest)
    {
        $value = null;
        switch($function)
        {
            case 'conditional':
                $value = $this->generateConditionalValue($spec[$function], $dest);
                break;
            case 'replace':
                $value = $this->generateReplaceValue($spec[$function], $dest);
                break;
            case 'arithmetic':
                $value = $this->computeArithmeticValue($spec[$function], $dest);
                break;
        }

        return $value;
    }

    /**
     * @param array $equation
     * @param array $dest
     * @return float|int|null
     * @throws \InvalidArgumentException
     */
    protected function computeArithmeticValue(array $equation, array &$dest)
    {
        if(count($equation) < 3)
        {
            throw new \InvalidArgumentException("Equations must consist of an array with 3 values");
        }
        if(!in_array($equation[1], self::$arithmeticOperators))
        {
            throw new \InvalidArgumentException("Invalid arithmetic operator");
        }

        $left = $this->rewriteVariableValue($equation[0], $dest, 'numeric');
        $right = $this->rewriteVariableValue($equation[2], $dest, 'numeric');
        if(is_array($left))
        {
            $left = $this->computeArithmeticValue($left, $dest);
        }
        if(is_array($right))
        {
            $right = $this->computeArithmeticValue($right, $dest);
        }

        switch($equation[1])
        {
            case "+":
                $value = $left + $right;
                break;
            case "-":
                $value = $left - $right;
                break;
            case "*":
                $value = $left * $right;
                break;
            case "/":
                $value = $left / $right;
                break;
            case "%":
                $value = $left % $right;
                break;
            default:
                $value = null;
        }
        return $value;
    }

    /**
     * @param array $replaceSpec The replace value spec
     * @param array $dest The table row document to save
     * @return mixed
     */
    protected function generateReplaceValue(array $replaceSpec, array &$dest)
    {
        $search = null;
        $replace = null;
        $subject = null;
        if(isset($replaceSpec['search']))
        {
            $search = $this->rewriteVariableValue($replaceSpec['search'], $dest);
        }
        if(isset($replaceSpec['replace']))
        {
            $replace = $this->rewriteVariableValue($replaceSpec['replace'], $dest);
        }
        if(isset($replaceSpec['subject']))
        {
            $subject = $this->rewriteVariableValue($replaceSpec['subject'], $dest);
        }

        return str_replace($search, $replace, $subject);
    }

    /**
     * @param array $conditionalSpec The conditional spec
     * @param array $dest The table row document to save
     * @return mixed The computed value
     */
    protected function generateConditionalValue(array $conditionalSpec, array &$dest)
    {
        $value = null;
        if(isset($conditionalSpec['if']) && is_array($conditionalSpec['if']))
        {
            $left = null;
            $operator = null;
            $right = null;
            if(isset($conditionalSpec['if'][0]))
            {
                $left = $this->rewriteVariableValue($conditionalSpec['if'][0], $dest);
            }
            if(isset($conditionalSpec['if'][1]))
            {
                $operator = $conditionalSpec['if'][1];
            }
            if(isset($conditionalSpec['if'][2]))
            {
                $right = $this->rewriteVariableValue($conditionalSpec['if'][2], $dest);
            }

            $bool = $this->doConditional($left, $operator, $right);

            $path = ($bool ? 'then' : 'else');

            if(isset($conditionalSpec[$path]))
            {
                if(is_array($conditionalSpec[$path]))
                {
                    $nestedComputedFunctions = array_intersect(self::$computedFieldFunctions, array_keys($conditionalSpec[$path]));
                    // This is 'just a regular old array'
                    if(empty($nestedComputedFunctions))
                    {
                        return $this->rewriteVariableValue($conditionalSpec[$path], $dest);
                    }

                    return $this->getComputedValue($nestedComputedFunctions[0], $conditionalSpec[$path], $dest);

                }
                else
                {
                    return $this->rewriteVariableValue($conditionalSpec[$path], $dest);
                }
            }

        }
        return $value;
    }

    /**
     * @param mixed $value The value to replace, if it contains a variable
     * @param array $dest The table row document to save
     * @param string|null $setType Force the return to be set to specified type
     * @return mixed
     */
    protected function rewriteVariableValue($value, array &$dest, $setType=null)
    {
        if(is_string($value))
        {
            if(strpos($value, '$') === 0)
            {
                $key =  str_replace('$','', $value);
                if(isset($dest[$key]))
                {
                    return $this->castValueType($dest[$key], $setType);
                }
                return null;
            }
            else
            {
                return $this->castValueType($value, $setType);
            }
        }
        elseif(is_array($value))
        {
            if($this->isFunction($value))
            {
                $function = array_keys($value);
                return $this->getComputedValue($function[0], $value, $dest);
            }
            $aryValue = array();
            foreach($value as $v)
            {
                $aryValue[] = $this->rewriteVariableValue($v, $dest);
            }
            return $aryValue;
        }

        return $this->castValueType($value, $setType);
    }

    /**
     * @param mixed $value
     * @return bool
     */
    protected function isFunction($value)
    {
        return (is_array($value) && count(array_keys($value)) === 1 && count(array_intersect(array_keys($value), self::$computedFieldFunctions)) ===1);
    }

    /**
     * @param mixed $value
     * @param string|null $type
     * @return mixed
     */
    protected function castValueType($value, $type=null)
    {
        switch($type)
        {
            case 'string':
                $value = (string)$value;
                break;
            case 'bool':
            case 'boolean':
                $value = (bool)$value;
                break;
            case 'numeric':
                if((!is_int($value)) && !is_float($value))
                {
                    if($value == (string)(int)$value)
                    {
                        $value = (int)$value;
                    }
                    else
                    {
                        $value = (float)$value;
                    }
                }
                break;
        }
        return $value;
    }

    /**
     * @param mixed $left The left value of the condition
     * @param string $operator The comparison operator
     * @param mixed $right The right value of the condition
     * @return bool
     * @throws \InvalidArgumentException
     */
    protected function doConditional($left, $operator, $right)
    {
        if((!empty($operator)) && !in_array($operator, self::$conditionalOperators))
        {
            throw new \InvalidArgumentException("Invalid conditional operator");
        }
        elseif(!$operator)
        {
            return ($left ? true : false);
        }
        $result = false;
        switch($operator)
        {
            case ">":
                $result = $left > $right;
                break;
            case ">=":
                $result = $left >= $right;
                break;
            case "<":
                $result = $left < $right;
                break;
            case "<=":
                $result = $left <= $right;
                break;
            case "==":
                $result = $left == $right;
                break;
            case "!=":
                $result = $left != $right;
                break;
            case "contains":
            case "not contains":
                if(is_array($left))
                {
                    $bool = in_array($right, $left);
                }
                else
                {

                    $bool = (strpos((string)$left, (string)$right) !== false);
                }
                $result = ($bool && $operator != "not contains");
                break;
            case "~=":
            case "!~":
                $match = preg_match($right, $left);
                $result = ($match > 0 && $operator != "!~");
                break;

        }
        return $result;
    }

    /**
     * Add fields to a table row
     * @param array $source
     * @param array $spec
     * @param array $dest
     * @access protected
     * @return void
     */
    protected function addFields($source,$spec,&$dest)
    {
        if (isset($spec['fields']))
        {
            foreach ($spec['fields'] as $f)
            {
                if(isset($f['temporary']) && $f['temporary'] === true)
                {
                    if(!in_array($f['fieldName'], $this->temporaryFields))
                    {
                        $this->temporaryFields[] = $f['fieldName'];
                    }
                }
                if(isset($f['predicates']))
                {
                    foreach ($f['predicates'] as $p)
                    {
                        if (is_string($p) && isset($source[$p]))
                        {
                            // Predicate is referenced directly
                            $this->generateValues($source, $f, $p, $dest);
                        } else
                        {
                            // Get a list of functions to run over a predicate - reverse it
                            $predicateFunctions = $this->getPredicateFunctions($p);
                            $predicateFunctions = array_reverse($predicateFunctions);

                            foreach($predicateFunctions as $function => $functionOptions)
                            {
                                // If we've got values then we're the innermost function, so we need to get the values
                                if($function == 'predicates')
                                {
                                    foreach($functionOptions as $v)
                                    {
                                        $v = trim($v);
                                        if (isset($source[$v]))
                                        {
                                            $this->generateValues($source, $f, $v, $dest);
                                        }
                                    }
                                // Otherwise apply a modifier
                                }
                                else
                                {
                                    if(isset($dest[$f['fieldName']]))
                                    {
                                        $dest[$f['fieldName']] = $this->applyModifier($function, $dest[$f['fieldName']], $functionOptions);
                                    }
                                }
                            }
                        }
                    }
                }

                // Allow URI linking to the ID
                if(isset($f['value']))
                {
                    if($f['value'] == '_link_' || $f['value'] == 'link' )
                    {
                        if($f['value'] == '_link_')
                        {
                            $this->warningLog("Table spec value '_link_' is deprecated", $f);
                        }
                        // If value exists, set as array
                        if(isset($dest[$f['fieldName']]))
                        {
                            if(!is_array($dest[$f['fieldName']]))
                            {
                                $dest[$f['fieldName']] = array($dest[$f['fieldName']]);
                            }
                            $dest[$f['fieldName']][] = $this->labeller->qname_to_alias($source['_id']['r']);
                        } else {
                            $dest[$f['fieldName']] = $this->labeller->qname_to_alias($source['_id']['r']);
                        }
                    }
                }
            }
        }
    }

    /**
     * Generate values for a given predicate
     * @param array $source
     * @param array $f
     * @param string $predicate
     * @param array $dest
     * @access protected
     * @return void
     */
    protected function generateValues($source, $f, $predicate, &$dest)
    {
        $values = array();
        if (isset($source[$predicate][VALUE_URI]) && !empty($source[$predicate][VALUE_URI]))
        {
            $values[] = $source[$predicate][VALUE_URI];
        }
        else if (isset($source[$predicate][VALUE_LITERAL]) && !empty($source[$predicate][VALUE_LITERAL]))
        {
            $values[] = $source[$predicate][VALUE_LITERAL];
        }
        else if (isset($source[$predicate][_ID_RESOURCE])) // field being joined is the _id, will have _id{r:'',c:''}
        {
            $values[] = $source[$predicate][_ID_RESOURCE];
        }
        else
        {
            foreach ($source[$predicate] as $v)
            {
                if (isset($v[VALUE_LITERAL]) && !empty($v[VALUE_LITERAL]))
                {
                    $values[] = $v[VALUE_LITERAL];
                }
                else if (isset($v[VALUE_URI]) && !empty($v[VALUE_URI]))
                {
                    $values[] = $v[VALUE_URI];
                }
                // _id's shouldn't appear in value arrays, so no need for third condition here
            }
        }

        // now add all the values
        foreach ($values as $v)
        {
            if (!isset($dest[$f['fieldName']]))
            {
                // single value
                $dest[$f['fieldName']] = $v;
            }
            else if (is_array($dest[$f['fieldName']]))
            {
                // add to existing array of values
                $dest[$f['fieldName']][] = $v;
            }
            else
            {
                // convert from single value to array of values
                $existingVal = $dest[$f['fieldName']];
                $dest[$f['fieldName']] = array();
                $dest[$f['fieldName']][] = $existingVal;
                $dest[$f['fieldName']][] = $v;
            }
        }
    }

    /**
     * Recursively get functions that can modify a predicate
     * @param array $array
     * @access protected
     * @return array
     */
    protected function getPredicateFunctions($array)
    {
        $predicateFunctions = array();
        if(is_array($array))
        {
            if(isset($array['predicates']))
            {
                $predicateFunctions['predicates'] = $array['predicates'];
            } else
            {
                $predicateFunctions[key($array)] = $array[key($array)];
                $predicateFunctions = array_merge($predicateFunctions, $this->getPredicateFunctions($array[key($array)]));
            }
        }
        return $predicateFunctions;
    }

    /**
     * Apply a specific modifier
     * Options you can use are
     *      lowercase - no options
     *      join - pass in "glue":" " to specify what to glue multiple values together with
     *      date - no options
     * @param string $modifier
     * @param string $value
     * @param array $options
     * @throws \Exception
     * @return mixed
     */
    private function applyModifier($modifier, $value, $options = array())
    {
        try
        {
            switch($modifier)
            {
                case 'predicates':
                    // Used to generate a list of values - does nothing here
                    break;
                case 'lowercase':
                    if(is_array($value))
                    {
                        $value = array_map('strtolower', $value);
                    } else
                    {
                        $value = strtolower($value);
                    }
                    break;
                case 'join':
                    if(is_array($value)) $value = implode($options['glue'], $value);
                    break;
                case 'date':
                    if(is_string($value)) $value = new \MongoDate(strtotime($value));
                    break;
                default:
                    throw new \Exception("Could not apply modifier:".$modifier);
                    break;
            }
        } catch(\Exception $e)
        {
            throw $e;
        }
        return $value;
    }

    /**
     * @param array $source
     * @param array $joins
     * @param array $dest
     * @param string $from
     * @param string $contextAlias
     */
    protected function doJoins($source,$joins,&$dest,$from,$contextAlias)
    {
        $this->expandSequence($joins,$source);
        foreach ($joins as $predicate=>$ruleset)
        {
            if (isset($source[$predicate]))
            {
                // todo: perhaps we can get better performance by detecting whether or not
                // the uri to join on is already in the impact index, and if so not attempting
                // to join on it. However, we need to think about different combinations of
                // nested joins in different points of the view spec and see if this would
                // complicate things. Needs a unit test or two.
                $joinUris = array();
                if (isset($source[$predicate][VALUE_URI]))
                {
                    // single value for join
                    $joinUris[] = array(_ID_RESOURCE=>$source[$predicate][VALUE_URI],_ID_CONTEXT=>$contextAlias);
                }
                else if($predicate == '_id')
                {
                    // single value for join
                    $joinUris[] = array(_ID_RESOURCE=>$source[$predicate][_ID_RESOURCE],_ID_CONTEXT=>$contextAlias);
                }
                else
                {
                    // multiple values for join
                    foreach ($source[$predicate] as $v)
                    {
                        $joinUris[] = array(_ID_RESOURCE=>$v[VALUE_URI],_ID_CONTEXT=>$contextAlias);
                    }
                }

                $recursiveJoins = array();
                $collection = (isset($ruleset['from'])
                    ? $this->config->getCollectionForCBD($this->storeName, $ruleset['from'])
                    : $this->config->getCollectionForCBD($this->storeName, $from)
                );
                $cursor = $collection->find(array('_id'=>array('$in'=>$joinUris)));
                $cursor->timeout(\Tripod\Mongo\Config::getInstance()->getMongoCursorTimeout());

                $this->addIdToImpactIndex($joinUris, $dest);
                foreach($cursor as $linkMatch) {

                    $this->addFields($linkMatch,$ruleset,$dest);

                    if (isset($ruleset['counts']))
                    {
                        $this->doCounts($linkMatch,$ruleset['counts'],$dest);
                    }

                    if (isset($ruleset['joins']))
                    {
                        // recursive joins must be done after this cursor has completed, otherwise things get messy
                        $recursiveJoins[] = array('data'=>$linkMatch, 'ruleset'=>$ruleset['joins']);
                    }
                }

                if (count($recursiveJoins)>0)
                {
                    foreach ($recursiveJoins as $r)
                    {
                        $this->doJoins($r['data'],$r['ruleset'],$dest,$from,$contextAlias);
                    }
                }
            }
        }
    }

    /**
     * Add counts to $dest by counting what is in $source according to $countSpec
     * @param $source
     * @param $countSpec
     * @param $dest
     */
    protected function doCounts($source, $countSpec, &$dest)
    {
        // process count aggregate function
        foreach ($countSpec as $c)
        {
            $fieldName = $c['fieldName'];
            if(isset($c['temporary']) && $c['temporary'] === true)
            {
                if(!in_array($fieldName, $this->temporaryFields))
                {
                    $this->temporaryFields[] = $fieldName;
                }
            }
            $applyRegex = (isset($c['regex'])) ? isset($c['regex']) : null;
            $count = 0;
            // just count predicates at current location
            if (isset($source[$c['property']]))
            {
                if (isset($source[$c['property']][VALUE_URI]) || isset($source[$c['property']][VALUE_LITERAL]))
                {
                    if ($applyRegex != null)
                    {
                        $count = $this->applyRegexToValue($c['regex'],$source[$c['property']]);
                    } else {
                        $count = 1;
                    }
                }
                else
                {
                    if ($applyRegex != null)
                    {
                        foreach ($source[$c['property']] as $value)
                        {
                            if ($this->applyRegexToValue($c['regex'],$value)) {
                                $count++;
                            }
                        }
                    }
                    else
                    {
                        $count = count($source[$c['property']]);
                    }
                }
            }
            if (!isset($dest[$fieldName])) $dest[$fieldName] = 0;
            $dest[$fieldName] += $count;
        }
    }

    /**
     * Test if the a particular type appears in the array of types associated with a particular spec and that the changeset
     * includes rdf:type (or is empty, meaning addition or deletion vs. update)
     * @param string $rdfType
     * @param array $validTypes
     * @param array $subjectPredicates
     * @return bool
     */
    protected function checkIfTypeShouldTriggerOperation($rdfType, array $validTypes, Array $subjectPredicates)
    {
        // We don't know if this is an alias or a fqURI, nor what is in the valid types, necessarily
        $types = array($rdfType);
        try
        {
            $types[] = $this->labeller->qname_to_uri($rdfType);
        }
        catch(\Tripod\Exceptions\LabellerException $e) {}
        try
        {
            $types[] = $this->labeller->uri_to_alias($rdfType);
        }
        catch(\Tripod\Exceptions\LabellerException $e) {}

        $intersectingTypes = array_unique(array_intersect($types, $validTypes));
        if(!empty($intersectingTypes))
        {
            // Table rows only need to be invalidated if their rdf:type property has changed
            // This means we're either adding or deleting a graph
            if(empty($subjectPredicates))
            {
                return true;
            }
            // Check for alias in changed predicates
            elseif(in_array('rdf:type', $subjectPredicates))
            {
                return true;
            }
            // Check for fully qualified URI in changed predicates
            elseif(in_array(RDF_TYPE, $subjectPredicates))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensure $indexToEnsure on the given mongo $collection
     * @param array $collection
     * @param \MongoCollection $indexToEnsure
     */
    protected function ensureIndex(\MongoCollection $collection,array $indexToEnsure)
    {
        $collection->ensureIndex(
            $indexToEnsure,
            array(
                'background'=>1
            )
        );

    }
    /**
     * Apply a regex to the RDF property value defined in $value
     * @param $regex
     * @param $value
     * @throws \Tripod\Exceptions\Exception
     * @return int
     */
    private function applyRegexToValue($regex, $value)
    {
        if (isset($value[VALUE_URI]) || isset($value[VALUE_LITERAL]))
        {
            $v = ($value[VALUE_URI]) ? $value[VALUE_URI] : $value[VALUE_LITERAL];
            return preg_match($regex, $v);
        }
        else
        {
            throw new \Tripod\Exceptions\Exception("Was expecting either VALUE_URI or VALUE_LITERAL when applying regex to value - possible data corruption with: ".var_export($value,true));
        }
    }
}
