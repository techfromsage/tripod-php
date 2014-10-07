<?php
/**
 * Holds the global configuration for Tripod
 */
class MongoTripodConfig
{
    /**
     * @var MongoTripodConfig
     */
    private static $instance = null;

    /**
     * @var array
     */
    private static $config = null;

    /**
     * @var MongoTripodLabeller
     */
    private $labeller = null;

    /**
     * @var string
     */
    private $defaultContext = null;

    /**
     * The defined database indexes, keyed by database name
     * @var array
     */
    private $indexes = array();

    /**
     * @var array
     */
    private $cardinality = array();

    /**
     * The connection strings for each defined database
     * @var array
     */
    private $dbConfig = array();

    /**
     * All of the defined viewSpecs
     * @var array
     */
    private $viewSpecs = array();

    /**
     * All of the defined tableSpecs
     * @var array
     */
    private $tableSpecs = array();

    /**
     * All of the defined searchDocSpecs
     * @var array
     */
    protected $searchDocSpecs = array();

    /**
     * Defined database configuration: dbname, collections, etc.
     * @var array
     */
    private $databases = array();

    /**
     * All defined namespaces
     *
     * @var array
     */
    protected $ns = array();

    /**
     * The transaction log db config
     * @var array
     */
    protected $tConfig = array();

    /**
     * Queue db config
     * @var array
     */
    protected $queueConfig = array();

    /**
     * This should be the name of a class that implement iTripod
     * @var string
     */
    protected $searchProviderClassName = null;

    /**
     * All of the predicates associated with a particular spec document
     *
     * @var array
     */
    protected $specPredicates;

    /**
     * MongoTripodConfig should not be instantiated directly: use MongoTripodConfig::getInstance()
     */
    private function __construct() {}

    /**
     * Used to load the config from self::config when new instance is generated
     *
     * @param array $config
     * @throws MongoTripodConfigException
     */
    private function loadConfig(Array $config)
    {
        if (array_key_exists('namespaces',$config))
        {
            $this->ns = $config['namespaces'];
        }

        $this->defaultContext = $this->getMandatoryKey("defaultContext",$config);

        $transactionConfig = $this->getMandatoryKey("transaction_log",$config);
        $this->tConfig["database"] = $this->getMandatoryKey("database",$transactionConfig,'transaction_log');
        $this->tConfig["collection"] = $this->getMandatoryKey("collection",$transactionConfig,'transaction_log');
        $this->tConfig["connStr"] = $this->getMandatoryKey("connStr",$transactionConfig,'transaction_log');
        if(array_key_exists("replicaSet", $transactionConfig) && !empty($transactionConfig["replicaSet"]))
        {
            $this->tConfig['replicaSet'] = $transactionConfig["replicaSet"];
        }

        $searchConfig = (array_key_exists("search_config",$config)) ? $config["search_config"] : array();
        if(!empty($searchConfig)){
            $this->searchProviderClassName = $this->getMandatoryKey('search_provider', $searchConfig, 'search');
            // Load search doc specs if search_config is set
            $searchDocSpecs = $this->getMandatoryKey('search_specifications', $searchConfig, 'search');
            foreach ($searchDocSpecs as $spec)
            {
                $this->searchDocSpecs[$spec[_ID_KEY]] = $spec;
            }
        }

        $queueConfig = $this->getMandatoryKey("queue",$config);
        $this->queueConfig["database"] = $this->getMandatoryKey("database",$queueConfig,'queue');
        $this->queueConfig["collection"] = $this->getMandatoryKey("collection",$queueConfig,'queue');
        $this->queueConfig["connStr"] = $this->getMandatoryKey("connStr",$queueConfig,'queue');
        if(array_key_exists("replicaSet", $queueConfig) && !empty($queueConfig["replicaSet"])) {
            $this->queueConfig['replicaSet'] = $queueConfig["replicaSet"];
        }

        // Load view specs
        $viewSpecs = (array_key_exists("view_specifications",$config)) ? $config["view_specifications"] : array();
        foreach ($viewSpecs as $spec)
        {
            $this->ifCountExistsWithoutTTLThrowException($spec);
            $this->viewSpecs[$spec[_ID_KEY]] = $spec;
        }

        // Load table specs
        $tableSpecs = (array_key_exists("table_specifications",$config)) ? $config["table_specifications"] : array();
        foreach ($tableSpecs as $spec)
        {
            if(!isset($spec[_ID_KEY]))
            {
                throw new MongoTripodConfigException("Table spec does not contain " . _ID_KEY);
            }

            // Get all "fields" in the spec
            $fieldsInTableSpec = $this->findFieldsInTableSpec('fields', $spec);
            // Loop through fields and validate
            foreach($fieldsInTableSpec as $field)
            {
                if (!isset($field['fieldName']))
                {
                    throw new MongoTripodConfigException("Field spec does not contain fieldName");
                }

                if(isset($field['predicates']))
                {
                    foreach($field['predicates'] as $p)
                    {
                        // If predicates is an array we've got modifiers
                        if(is_array($p))
                        {
                            /*
                             * checkModifierFunctions will check if each predicate modifier is valid - it will
                             * check recursively through the predicate
                             */
                            $this->checkModifierFunctions($p, MongoTripodTables::$predicateModifiers);
                        }
                    }
                }
                // fields can either have predicates or values
                elseif((!isset($field['value'])) || empty($field['value']))
                {
                    throw new MongoTripodConfigException("Field spec does not contain predicates or value");
                }
            }

            // Get all "counts" in the spec
            $fieldsInTableSpec = $this->findFieldsInTableSpec('counts', $spec);
            // Loop through fields and validate
            foreach($fieldsInTableSpec as $field)
            {
                if (!isset($field['fieldName']))
                {
                    throw new MongoTripodConfigException("Count spec does not contain fieldName");
                }

                if(isset($field['property']))
                {
                    if (!is_string($field['property']))
                    {
                        throw new MongoTripodConfigException("Count spec property was not a string");
                    }
                }
                else
                {
                    throw new MongoTripodConfigException("Count spec does not contain property");
                }
            }
            $this->tableSpecs[$spec[_ID_KEY]] = $spec;
        }

        $this->databases = $this->getMandatoryKey("databases",$config);
        foreach ($this->databases as $dbName=>$db)
        {
            $this->dbConfig[$dbName] = array ("connStr"=>$this->getMandatoryKey("connStr",$db));
            if(isset($db['replicaSet']) && !empty($db['replicaSet']))
            {
                $this->dbConfig[$dbName]["replicaSet"]=$db['replicaSet'];
            }
            $this->cardinality[$dbName] = array();
            $this->indexes[$dbName] = array();
            foreach($db["collections"] as $collName=>$collection)
            {
                // Set cardinality, also checking against defined namespaces
                if (array_key_exists('cardinality', $collection))
                {
                    // Test that the namespace exists for each cardinality rule defined
                    $cardinality = $collection['cardinality'];
                    foreach ($cardinality as $qname=>$cardinalityValue)
                    {
                        // just grab the first element
                        $namespace  = array_shift(explode(':', $qname));

                        if (array_key_exists($namespace, $this->ns))
                        {
                            $this->cardinality[$dbName][$collName][] = $cardinality;
                        }
                        else
                        {
                            throw new MongoTripodConfigException("Cardinality '{$qname}' does not have the namespace defined");
                        }
                    }
                }
                else
                {
                    $this->cardinality[$dbName][$collName] = array();
                }

                $this->cardinality[$dbName][$collName] = (array_key_exists("cardinality",$collection)) ? $collection['cardinality'] : array();

                // Ensure indexes are legal
                if (array_key_exists("indexes",$collection))
                {
                    $this->indexes[$dbName][$collName] = array();
                    foreach($collection["indexes"] as $indexName=>$indexFields)
                    {
                        // check no more than 1 indexField is an array to ensure Mongo will be able to create compount indexes
                        if (count($indexFields)>1)
                        {
                            $fieldsThatAreArrays = 0;
                            foreach ($indexFields as $field=>$fieldVal)
                            {
                                $cardinalityField = preg_replace('/\.value/','',$field);
                                if (!array_key_exists($cardinalityField,$this->cardinality[$dbName][$collName])||$this->cardinality[$dbName][$collName][$cardinalityField]!=1)
                                {
                                    $fieldsThatAreArrays++;
                                }
                                if ($fieldsThatAreArrays>1)
                                {
                                    throw new MongoTripodConfigException("Compound index $indexName has more than one field with cardinality > 1 - mongo will not be able to build this index");
                                }
                            }
                        } // @codeCoverageIgnoreStart
                        // @codeCoverageIgnoreEnd

                        $this->indexes[$dbName][$collName][$indexName] = $indexFields;
                    }
                }
            }
        }

    }

    /**
     * Creates an associative array of all predicates/properties associated with all table and search document specifications
     * @return array
     */
    protected function getDefinedPredicatesInSpecs()
    {
        $predicates = array();
        $specs = array_merge($this->getTableSpecifications(), $this->getSearchDocumentSpecifications());
        foreach($specs as $spec)
        {
            if(!isset($spec[_ID_KEY]))
            {
                continue;
            }
            $predicates[$spec[_ID_KEY]] = array_unique($this->getDefinedPredicatesInSpecBlock($spec));
        }

        return $predicates;
    }

    /**
     * Recursively crawls a configuration document array (or part of one) and returns any associated predicates/properties
     * @param array $block
     * @return array
     */
    protected function getDefinedPredicatesInSpecBlock(array $block)
    {
        $predicates = array();
        // If the spec has a "type" property, include rdf:type
        if(isset($block['type']))
        {
            $predicates[] = $this->getLabeller()->uri_to_alias(RDF_TYPE);
        }
        if(isset($block['filter']))
        {
            foreach($block['filter'] as $filter)
            {
                if(isset($filter['condition']))
                {
                    $predicates = array_merge($predicates, $this->getPredicatesFromFilterCondition($filter['condition']));
                }
            }
        }
        // Get the predicates out of the defined fields
        if(isset($block['fields']))
        {
            foreach($block['fields'] as $field)
            {
                if(isset($field['predicates']))
                {
                    foreach($field['predicates'] as $p)
                    {
                        if(!empty($p))
                        {
                            // The actual predicate strings may be buried in predicate function blocks
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }

        // Loop through the joins and pass the objects back to this method
        if(isset($block['joins']))
        {
            foreach($block['joins'] as $predicate=>$join)
            {
                // Joins are keyed on the predicate, so save that
                $predicates[] = $this->getLabeller()->uri_to_alias($predicate);
                $predicates = array_merge($predicates, $this->getDefinedPredicatesInSpecBlock($join));
            }
        }

        // Loop through the counts blocks
        if(isset($block['counts']))
        {
            foreach($block['counts'] as $property)
            {
                // counts use the redundant property 'property', which behaves exactly like a predicate and needs to be deprecated
                if(isset($property['property']))
                {
                    $predicates[] = $this->getLabeller()->uri_to_alias($property['property']);
                }

                // This is here so we can easily deprecate 'property' in favor of 'predicates'
                if(isset($property['predicates']))
                {
                    foreach($property['predicates'] as $p)
                    {
                        if(!empty($p))
                        {
                            // The actual predicate strings may be buried in predicate function blocks
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }

        // Loop through the indices: these should be more or less identical to 'fields'
        if(isset($block['indices']))
        {
            foreach($block['indices'] as $index)
            {
                if(isset($index['predicates']))
                {
                    foreach($index['predicates'] as $p)
                    {
                        if(!empty($p))
                        {
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }
        return $predicates;
    }

    /**
     * Rewrites any predicate uris to alias curies
     * @param string|array $predicate
     * @return array
     */
    protected function getPredicateAliasesFromPredicateProperty($predicate)
    {
        $predicates = array();
        if(is_string($predicate) && !empty($predicate))
        {
            $predicates[] = $this->getLabeller()->uri_to_alias($predicate);
        }
        elseif(is_array($predicate))
        {
            foreach($this->getPredicatesFromPredicateFunctions($predicate) as $p)
            {
                $predicates[] = $this->getLabeller()->uri_to_alias($p);
            }
        }

        return $predicates;
    }

    /**
     * When given an array as input, will traverse any predicate functions and return the predicate strings
     *
     * @param array $array
     * @return array
     */
    protected function getPredicatesFromPredicateFunctions($array)
    {
        $predicates = array();
        if(is_array($array))
        {
            if(isset($array['predicates']))
            {
                $predicates = $array['predicates'];
            } else
            {
                $predicates = array_merge($predicates, $this->getPredicatesFromPredicateFunctions($array[key($array)]));
            }
        }
        return $predicates;
    }

    /**
     * Parses a specDocument's "filter" parameter for any predicates
     * @param $filter
     * @return array
     */
    protected function getPredicatesFromFilterCondition($filter)
    {
        $predicates = array();
        $regex = "/(^|\b)(\w+\:\w+)\.(l|u)(\b|$)/";
        foreach($filter as $key=>$condition)
        {
            if(is_string($key))
            {
                $numMatches = preg_match_all($regex, $key, $matches);
                for($i = 0; $i < $numMatches; $i++)
                {
                    if(isset($matches[2][$i]))
                    {
                        $predicates[] = $matches[2][$i];
                    }
                }
            }
            if(is_string($condition))
            {
                $numMatches = preg_match_all($regex, $condition, $matches);
                for($i = 0; $i < $numMatches; $i++)
                {
                    if(isset($matches[2][$i]))
                    {
                        $predicates[] = $matches[2][$i];
                    }
                }
            }
            elseif(is_array($condition))
            {
                array_merge($predicates, $this->getPredicatesFromFilterCondition($condition));
            }
        }
        return $predicates;
    }


    /**
     * Returns an array of associated predicates in a table or search document specification
     * Note: will not return viewSpec predicates
     *
     * @param string $specId
     * @return array
     */
    public function getDefinedPredicatesInSpec($specId)
    {
        if(!isset($this->specPredicates))
        {
            $this->specPredicates = $this->getDefinedPredicatesInSpecs();
        }
        if(isset($this->specPredicates[$specId]))
        {
            return $this->specPredicates[$specId];
        }
        return array();
    }

    /**
     * Check modifier functions against fields
     * @param array $array
     * @param mixed $parent
     * @param string|null $parentKey
     * @throws MongoTripodConfigException
     */
    public function checkModifierFunctions(array $array, $parent, $parentKey = null)
    {
        foreach($array as $k => $v)
        {
            // You can have recursive modifiers so we check if the value is an array.
            if(is_array($v))
            {
                // Check config
                // Valid configs can be top level modifiers and their attributes inside - you can have a top level modifier
                //      inside a top level modifier - that's why we also check MongoTripodTables::$predicatesModifiers direct
                if(!array_key_exists($k, $parent) && !array_key_exists($k, MongoTripodTables::$predicateModifiers))
                {
                    throw new MongoTripodConfigException("Invalid modifier: '".$k."' in key '".$parentKey."'");
                }

                // If this config value is a top level modifier, use that as the parent so that we can check the attributes
                if(array_key_exists($k, MongoTripodTables::$predicateModifiers))
                {
                    $this->checkModifierFunctions($v, MongoTripodTables::$predicateModifiers[$k], $k);
                } else
                {
                    $this->checkModifierFunctions($v, $parent[$k], $k);
                }


            } else if(is_string($k))
            {
                // Check key
                if(!array_key_exists($k, $parent))
                {
                    throw new MongoTripodConfigException("Invalid modifier: '".$k."' in key '".$parentKey."'");
                }
            }
        }
    }

    /**
     * @codeCoverageIgnore
     * @static
     * @return Array|null
     */
    public static function getConfig()
    {
        return self::$config;
    }

    /**
     * Returns an alias curie of the default context (i.e. graph name)
     *
     * @return string
     */
    public function getDefaultContextAlias()
    {
        return $this->getLabeller()->uri_to_alias($this->defaultContext);
    }

    /**
     * Since this is a singleton class, use this method to create a new config instance.
     * @uses MongoTripodConfig::setConfig() Configuration must be set prior to calling this method. To generate a completely new object, set a new config
     * @codeCoverageIgnore
     * @static
     * @return MongoTripodConfig
     * @throws MongoTripodConfigException
     */
    public static function getInstance()
    {
        if (self::$config == null)
        {
            throw new MongoTripodConfigException("Call MongoTripodConfig::setConfig() first");
        }
        if (self::$instance == null)
        {
            self::$instance = new MongoTripodConfig();
            self::$instance->loadConfig(self::$config);
        }
        return self::$instance;
    }

    /**
     * set the config
     * @usedby MongoTripodConfig::getInstance()
     * @param array $config
     */
    public static function setConfig(Array $config)
    {
        self::$config = $config;
        self::$instance = null; // this will force a reload next time getInstance() is called
    }

    /**
     * Returns a list of the configured indexes grouped by collection
     * @param string $dbName
     * @return mixed
     */
    public function getIndexesGroupedByCollection($dbName)
    {
        $indexes = $this->indexes[$dbName];
        //TODO: if we have much more default indexes we should find a better way of doing this
        foreach($indexes as $collection=>$indices) {
            $indexes[$collection][_LOCKED_FOR_TRANS_INDEX] = array(_ID_KEY=>1, _LOCKED_FOR_TRANS=>1);
            $indexes[$collection][_UPDATED_TS_INDEX] = array(_ID_KEY=>1, _UPDATED_TS=>1);
            $indexes[$collection][_CREATED_TS_INDEX] = array(_ID_KEY=>1, _CREATED_TS=>1);
        }

        // also add the indexes for any views/tables
        $tableIndexes = array();
        foreach ($this->getTableSpecifications() as $tspec)
        {
            if (array_key_exists("ensureIndexes",$tspec))
            {
                foreach ($tspec["ensureIndexes"] as $index)
                {
                    $tableIndexes[] = $index;
                }
            }
        }
        $indexes[TABLE_ROWS_COLLECTION] = $tableIndexes;

        $viewIndexes = array();
        foreach ($this->getViewSpecifications() as $vspec)
        {
            if (array_key_exists("ensureIndexes",$vspec))
            {
                foreach ($vspec["ensureIndexes"] as $index)
                {
                    $viewIndexes[] = $index;
                }
            }
        }
        $indexes[VIEWS_COLLECTION] = $viewIndexes;

        return $indexes;
    }

    /**
     * Get the cardinality values for a DB/Collection.
     *
     * @param string $dbName The database name to use.
     * @param string $collName The collection in the database.
     * @param string $qName Either the qname to get the values for or empty for all cardinality values.
     * @return mixed If no qname is specified then returns an array of cardinality options, otherwise returns the cardinality value for the given qname.
     */
    public function getCardinality($dbName,$collName,$qName=null)
    {
        // If no qname specified the return all cardinality rules for this db/collection.
        if (empty($qName))
        {
            return $this->cardinality[$dbName][$collName];
        }

        // Return the cardinality rule for the specified qname.
        if (array_key_exists($qName,$this->cardinality[$dbName][$collName]))
        {
            return $this->cardinality[$dbName][$collName][$qName];
        }
        else
        {
            return -1;
        }
    }

    /**
     * Returns a boolean reflecting whether or not the database and collection are defined in the config
     * @param string $dbName
     * @param string $collName
     * @return bool
     */
    public function isCollectionWithinConfig($dbName,$collName)
    {
        return (array_key_exists($dbName,$this->databases)) ? array_key_exists($collName,$this->databases[$dbName]['collections']) : false;
    }

    /**
     * Returns an array of collection configurations for the supplied database name
     * @param string $dbName
     * @return array
     */
    public function getCollections($dbName)
    {
        return (array_key_exists($dbName,$this->databases)) ? $this->databases[$dbName]['collections'] : array();
    }

    /**
     * Returns the connection string for the supplied database name
     *
     * @param string $dbName
     * @return string
     * @throws MongoTripodConfigException
     */
    public function getConnStr($dbName)
    {
        if (array_key_exists($dbName,$this->dbConfig))
        {
            if($this->isReplicaSet($dbName)){
                // if this is a replica set then we have to make sure that the connstr specified
                // connects directly to the /admin database on the cluster.
                // see XIP-2448. All im doing here is checking to see that the connstr ends with /admin
                // substr is faster than regex match
                $connStr = $this->dbConfig[$dbName]["connStr"];
                if ($this->isConnectionStringValidForRepSet($connStr)){
                    return $connStr;
                } else {
                    throw new MongoTripodConfigException("Connection string for $dbName must include /admin database when connecting to Replica Set");
                }

            } else {
                return $this->dbConfig[$dbName]["connStr"];
            }
        }
        else
        {
            throw new MongoTripodConfigException("Database $dbName does not exist in configuration");
        }
    }

    /**
     * Returns the transaction log database connection string
     * @return string
     * @throws MongoTripodConfigException
     */
    public function getTransactionLogConnStr() {
        if(array_key_exists("replicaSet", $this->tConfig) && !empty($this->tConfig["replicaSet"])) {
            $connStr = $this->tConfig['connStr'];
            if ($this->isConnectionStringValidForRepSet($connStr)){
                return $connStr;
            } else {
                throw new MongoTripodConfigException("Connection string for Transaction Log must include /admin database when connecting to Replica Set");
            }
        } else {
            return $this->tConfig['connStr'];
        }
    }

    /**
     * Returns the queue database connection string
     *
     * @return string
     * @throws MongoTripodConfigException
     */
    public function getQueueConnStr() {
        if(array_key_exists("replicaSet", $this->queueConfig) && !empty($this->queueConfig["replicaSet"])) {
            $connStr = $this->queueConfig['connStr'];
            if ($this->isConnectionStringValidForRepSet($connStr)){
                return $connStr;
            } else {
                throw new MongoTripodConfigException("Connection string for Queue must include /admin database when connecting to Replica Set");
            }
        } else {
            return $this->queueConfig['connStr'];
        }
    }

    /**
     * Returns a replica set name for the database, if one has been defined
     * @param string|$dbName
     * @return string|null
     */
    public function getReplicaSetName($dbName)
    {
        if($this->isReplicaSet($dbName))
        {
            return $this->dbConfig[$dbName]['replicaSet'];
        }

        return null;
    }

    /**
     * Returns a boolean reflecting whether or not a replica set has been defined for the supplied database name
     * @param string $dbName
     * @return bool
     */
    public function isReplicaSet($dbName)
    {
        if (array_key_exists($dbName,$this->dbConfig))
        {
            if(array_key_exists("replicaSet",$this->dbConfig[$dbName]) && !empty($this->dbConfig[$dbName]["replicaSet"])) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return the view specification document for the supplied id, if it exists
     * @param string $vid
     * @return array|null
     */
    public function getViewSpecification($vid)
    {
        if (array_key_exists($vid,$this->viewSpecs))
        {
            return $this->viewSpecs[$vid];
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the search document specification for the supplied id, if it exists
     * @param string $sid
     * @return array|null
     */
    public function getSearchDocumentSpecification($sid)
    {
        if(array_key_exists($sid, $this->searchDocSpecs)) {
            return $this->searchDocSpecs[$sid];
        }

        return null;
    }

    /**
     * Returns an array of all search document specifications, or specification ids
     *
     * @param string|null $type When supplied, will only return search document specifications that are triggered by this rdf:type
     * @param bool $justReturnSpecId default is false. If true will only return an array of specification _id's, otherwise returns the array of specification documents
     * @return array
     */
    public function getSearchDocumentSpecifications($type=null, $justReturnSpecId=false)
    {
        $specs = array();

        if(empty($type)){
            if($justReturnSpecId){
                $specIds = array();
                foreach($this->searchDocSpecs as $spec){
                    $specIds[] = $spec[_ID_KEY];
                }
                return $specIds;
            } else {
                return $this->searchDocSpecs;
            }
        }

        $labeller = new MongoTripodLabeller();
        $typeAsUri = $labeller->uri_to_alias($type);
        $typeAsQName = $labeller->qname_to_alias($type);

        foreach ($this->searchDocSpecs as $spec)
        {
            if(is_array($spec[_ID_TYPE])){
                if(in_array($typeAsUri, $spec[_ID_TYPE]) || in_array($typeAsQName, $spec[_ID_TYPE])){
                    if($justReturnSpecId){
                        $specs[] = $spec[_ID_KEY];
                    } else {
                        $specs[] = $spec;
                    }
                }
            } else {
                if ($spec[_ID_TYPE]==$typeAsUri || $spec[_ID_TYPE]==$typeAsQName)
                {
                    if($justReturnSpecId){
                        $specs[] = $spec[_ID_KEY];
                    } else {
                        $specs[] = $spec;
                    }
                }
            }
        }
        return $specs;
    }

    /**
     * Returns the requested table specification, if it exists
     *
     * @param string $tid
     * @return array|null
     */
    public function getTableSpecification($tid)
    {
        if (array_key_exists($tid,$this->tableSpecs))
        {
            return $this->tableSpecs[$tid];
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns all defined table specifications
     * @codeCoverageIgnore
     * @return array
     */
    public function getTableSpecifications()
    {
        return $this->tableSpecs;
    }

    /**
     * Returns all defined view specification
     * @codeCoverageIgnore
     * @return array
     */
    public function getViewSpecifications()
    {
        return $this->viewSpecs;
    }


    /**
     * This method returns a unique list of every rdf type configured in a specifications ['type'] restriction
     * @return array of types
     */
    public function getAllTypesInSpecifications()
    {
        $viewTypes   = $this->getTypesInViewSpecifications();
        $tableTypes  = $this->getTypesInTableSpecifications();
        $searchTypes = $this->getTypesInSearchSpecifications();
        $types = array_unique(array_merge($viewTypes, $tableTypes, $searchTypes));
        return array_values($types);
    }

    /**
     * Returns a unique list of every rdf type configured in the view spec ['type'] restriction
     * @param string|null $collectionName
     * @return array
     */
    public function getTypesInViewSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getViewSpecifications(), $collectionName));
    }

    /**
     * Returns a unique list of every rdf type configured in the table spec ['type'] restriction
     * @param string|null $collectionName
     * @return array
     */
    public function getTypesInTableSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getTableSpecifications(), $collectionName));
    }

    /**
     * Returns a unique list of every rdf type configured in the search doc spec ['type'] restriction
     * @param string|null $collectionName
     * @return array
     */
    public function getTypesInSearchSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getSearchDocumentSpecifications(), $collectionName));
    }

    /**
     * Returns an array of database names
     * @return array
     */
    public function getDbs()
    {
        return array_keys($this->dbConfig);
    }

    /**
     * This method was added to allow us to test the getInstance() method
     * @codeCoverageIgnore
     */
    public function destroy()
    {
        self::$instance = NULL;
        self::$config = NULL;
    }

    /* PROTECTED FUNCTIONS */

    /**
     * @return MongoTripodLabeller
     */
    protected function getLabeller()
    {
        if ($this->labeller==null)
        {
            $this->labeller = new MongoTripodLabeller();
        }
        return $this->labeller;
    }

    /* PRIVATE FUNCTIONS */
    /**
     * Returns a unique list of every rdf type configured in the supplied specs' ['type'] restriction
     * @param array $specifications
     * @param string|null $collectionName
     * @return array
     */
    private function getSpecificationTypes(Array $specifications, $collectionName=null)
    {
        $types = array();
        foreach($specifications as $spec){

            if(!empty($collectionName)){
                if($spec['from'] !== $collectionName){
                    continue; // skip this view spec if it isnt for the collection
                }
            }

            if(is_array($spec[_ID_TYPE])){
                $types = array_merge($spec[_ID_TYPE], $types);
            } else {
                $types[] = $spec[_ID_TYPE];
            }
        }
        return $types;
    }

    /**
     * If we have 'counts' in a view spec, a 'ttl' must be defined.
     * Note: this does not apply to tables or search docs
     * @param array $spec
     * @throws MongoTripodConfigException
     */
    private function ifCountExistsWithoutTTLThrowException($spec)
    {
        if (array_key_exists("ttl",$spec))
        {
            return; // ttl exists
        }
        else
        {
            if (array_key_exists("joins",$spec))
            {
                // recurse
                foreach($spec['joins'] as $join)
                {
                    $this->ifCountExistsWithoutTTLThrowException($join);
                }
            }
            if (array_key_exists("counts",$spec))
            {
                throw new MongoTripodConfigException("Aggregate function counts exists in spec, but no TTL defined");
            }
            else
            {
                return;
            }
        }
    }

    /**
     * Returns the value of the supplied key or throws an error, if missing
     * @param string $key
     * @param array $a
     * @param string $configName
     * @return mixed
     * @throws MongoTripodConfigException
     */
    private function getMandatoryKey($key,Array $a,$configName='config')
    {
        if (!array_key_exists($key,$a))
        {
            throw new MongoTripodConfigException("Mandatory config key [$key] is missing from $configName");
        }
        return $a[$key];
    }

    /**
     * @param string $connStr
     * @return bool
     */
    private function isConnectionStringValidForRepSet($connStr)
    {
        $needle = "/admin";
        if (substr($connStr, -6) === $needle){
            return true;
        } else {
            return false;
        }
    }

    /**
     * Finds fields in a table specification
     * @param string $fieldName
     * @param array $spec, a part of space ot complete spec
     * @return array
     */
    private function findFieldsInTableSpec($fieldName, $spec)
    {
        $fields = array();
        if(is_array($spec) && !empty($spec))
        {
            if(array_key_exists($fieldName, $spec))
            {
                $fields = $spec[$fieldName];
            }

            if(isset($spec['joins']))
            {
                foreach($spec['joins'] as $join)
                {
                    $fields = array_merge($fields, $this->findFieldsInTableSpec($fieldName, $join));
                }
            }
        }
        return $fields;
    }

    /**
     * Returns an array of defined namespaces
     * @return array
     */
    public function getNamespaces()
    {
        return $this->ns;
    }

    /**
     * Getter for transaction log connection config 
     * @return array
     */
    public function getTransactionLogConfig()
    {
        return $this->tConfig;
    }

    /**
     * Returns the MongoTripodQueue connection config
     * @return array
     */
    public function getQueueConfig()
    {
        return $this->queueConfig;
    }

    /**
     * @return string
     */
    public function getSearchProviderClassName()
    {
        return $this->searchProviderClassName;
    }
}
class MongoTripodConfigException extends Exception {}