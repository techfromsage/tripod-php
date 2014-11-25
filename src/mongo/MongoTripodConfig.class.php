<?php
/**
 * Holds the global configuration for Tripod
 */
class MongoTripodConfig
{
    /**
     * @var MongoTripodConfig
     */
    private static $instance;

    /**
     * @var array
     */
    private static $config;

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
     * The value should be the name of a class that implement iTripodSearchProvider keyed by storename
     * @var array
     */
    protected $searchProviderClassName = array();

    /**
     * All of the predicates associated with a particular spec document
     *
     * @var array
     */
    protected $specPredicates;

    /**
     * A simple map between collection names and the database name they belong to
     * @var array
     */
    protected $collectionDatabases = array();

    /**
     * @var array
     */
    protected $activeMongoConnections = array();

    /**
     * @var string
     */
    protected $defaultDatabase;

    /**
     * @var array
     */
    protected $dataSources = array();

    protected $podConnections = array();

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
    protected function loadConfig(Array $config)
    {
        if (array_key_exists('namespaces',$config))
        {
            $this->ns = $config['namespaces'];
        }

        $this->defaultContext = $this->getMandatoryKey("defaultContext",$config);

        foreach($this->getMandatoryKey('data_sources', $config) as $source=>$c)
        {
            if(!array_key_exists('type', $c))
            {
                throw new MongoTripodConfigException("No type set for data source $source");
            }
            if(!array_key_exists('connection', $c))
            {
                throw new MongoTripodConfigException("No connection information set for data source $source");
            }
            $this->dataSources[$source] = $c;
        }

        $transactionConfig = $this->getMandatoryKey("transaction_log",$config);
        $this->tConfig["data_source"] = $this->getMandatoryKey('data_source', $transactionConfig, 'transaction_log');
        if(!isset($this->dataSources[$this->tConfig['data_source']]))
        {
            throw new MongoTripodConfigException("Transaction log data source, " . $this->tConfig['data_source'] . ", was not defined");
        }
        $this->tConfig["database"] = $this->getMandatoryKey("database",$transactionConfig,'transaction_log');
        $this->tConfig["collection"] = $this->getMandatoryKey("collection",$transactionConfig,'transaction_log');

        $queueConfig = $this->getMandatoryKey("queue",$config);
        $this->queueConfig["database"] = $this->getMandatoryKey("database",$queueConfig,'queue');
        $this->queueConfig["collection"] = $this->getMandatoryKey("collection",$queueConfig,'queue');
        $this->queueConfig["data_source"] = $this->getMandatoryKey("data_source",$queueConfig,'queue');
        if(!isset($this->dataSources[$this->queueConfig['data_source']]))
        {
            throw new MongoTripodConfigException("Queue data source, " . $this->queueConfig['data_source'] . ", was not defined");
        }

        // A 'pod' corresponds to a logical database
        $this->databases = $this->getMandatoryKey("stores",$config);
        $defaultDB = null;
        foreach ($this->databases as $storeName=>$storeConfig)
        {
            $this->dbConfig[$storeName] = array ("data_source"=>$this->getMandatoryKey("data_source",$storeConfig));
            if(isset($storeConfig['database']) && !empty($storeConfig['database']))
            {
                $this->dbConfig[$storeName]["database"]=$storeConfig['database'];
            }
            else
            {
                $this->dbConfig[$storeName]["database"] = $storeName;
            }

            $this->cardinality[$storeName] = array();
            $this->indexes[$storeName] = array();
            $this->podConnections[$storeName] = array();
            if(isset($storeConfig["pods"]))
            {
                foreach($storeConfig["pods"] as $podName=>$podConfig)
                {
                    $dataSource = (isset($podConfig['data_source']) ? $podConfig['data_source'] : $storeConfig['data_source']);
                    $this->podConnections[$storeName][$podName] = $dataSource;

                    // Set cardinality, also checking against defined namespaces
                    if (array_key_exists('cardinality', $podConfig))
                    {
                        // Test that the namespace exists for each cardinality rule defined
                        $cardinality = $podConfig['cardinality'];
                        foreach ($cardinality as $qname=>$cardinalityValue)
                        {
                            $namespaces = explode(':', $qname);
                            // just grab the first element
                            $namespace  = array_shift($namespaces);

                            if (array_key_exists($namespace, $this->ns))
                            {
                                $this->cardinality[$storeName][$podName][] = $cardinality;
                            }
                            else
                            {
                                throw new MongoTripodConfigException("Cardinality '{$qname}' does not have the namespace defined");
                            }
                        }
                    }
                    else
                    {
                        $this->cardinality[$storeName][$podName] = array();
                    }

                    $this->cardinality[$storeName][$podName] = (array_key_exists("cardinality",$podConfig)) ? $podConfig['cardinality'] : array();

                    // Ensure indexes are legal
                    if (array_key_exists("indexes",$podConfig))
                    {
                        $this->indexes[$storeName][$podName] = array();
                        foreach($podConfig["indexes"] as $indexName=>$indexFields)
                        {
                            // check no more than 1 indexField is an array to ensure Mongo will be able to create compount indexes
                            if (count($indexFields)>1)
                            {
                                $fieldsThatAreArrays = 0;
                                foreach ($indexFields as $field=>$fieldVal)
                                {
                                    $cardinalityField = preg_replace('/\.value/','',$field);
                                    if (!array_key_exists($cardinalityField,$this->cardinality[$storeName][$podName])||$this->cardinality[$storeName][$podName][$cardinalityField]!=1)
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

                            $this->indexes[$storeName][$podName][$indexName] = $indexFields;
                        }
                    }
                }
            }
            $searchConfig = (array_key_exists("search_config",$storeConfig)) ? $storeConfig["search_config"] : array();
            $this->searchDocSpecs[$storeName] = array();
            if(!empty($searchConfig)){
                $this->searchProviderClassName[$storeName] = $this->getMandatoryKey('search_provider', $searchConfig, 'search');
                // Load search doc specs if search_config is set
                $searchDocSpecs = $this->getMandatoryKey('search_specifications', $searchConfig, 'search');
                foreach ($searchDocSpecs as $spec)
                {
                    if(!isset($spec[_ID_KEY]))
                    {
                        throw new MongoTripodConfigException("Search document spec does not contain " . _ID_KEY);
                    }
                    if($this->searchProviderClassName[$storeName] == SEARCH_PROVIDER_MONGO)
                    {
                        if(isset($spec['to']))
                        {
                            if(!isset($this->dataSources[$spec['to']]))
                            {
                                throw new MongoTripodConfigException("'" . $spec[_ID_KEY] . "[\"to\"]' property references an undefined data source");
                            }
                        }
                        else
                        {
                            $spec['to'] = $storeConfig['data_source'];
                        }
                    }
                    $this->searchDocSpecs[$storeName][$spec[_ID_KEY]] = $spec;
                }
            }

            // Load view specs
            $viewSpecs = (array_key_exists("view_specifications",$storeConfig)) ? $storeConfig["view_specifications"] : array();
            $this->viewSpecs[$storeName] = array();
            foreach ($viewSpecs as $spec)
            {
                if(!isset($spec[_ID_KEY]))
                {
                    throw new MongoTripodConfigException("View spec does not contain " . _ID_KEY);
                }
                $this->ifCountExistsWithoutTTLThrowException($spec);
                if(isset($spec['to']))
                {
                    if(!isset($this->dataSources[$spec['to']]))
                    {
                        throw new MongoTripodConfigException("'" . $spec[_ID_KEY] . "[\"to\"]' property references an undefined data source");
                    }
                }
                else
                {
                    $spec['to'] = $storeConfig['data_source'];
                }
                $this->viewSpecs[$storeName][$spec[_ID_KEY]] = $spec;
            }

            // Load table specs
            $tableSpecs = (array_key_exists("table_specifications",$storeConfig)) ? $storeConfig["table_specifications"] : array();
            $this->tableSpecs[$storeName] = array();
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

                if(isset($spec['to']))
                {
                    if(!isset($this->dataSources[$spec['to']]))
                    {
                        throw new MongoTripodConfigException("'" . $spec[_ID_KEY] . "[\"to\"]' property references an undefined data source");
                    }
                }
                else
                {
                    $spec['to'] = $storeConfig['data_source'];
                }

                $this->tableSpecs[$storeName][$spec[_ID_KEY]] = $spec;
            }
        }


    }

    /**
     * Creates an associative array of all predicates/properties associated with all table and search document specifications
     * @param string $storename
     * @return array
     */
    protected function getDefinedPredicatesInSpecs($storename)
    {
        $predicates = array();
        $specs = array_merge($this->getTableSpecifications($storename), $this->getSearchDocumentSpecifications($storename));
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
     * @param string $storename
     * @param string $specId
     * @return array
     */
    public function getDefinedPredicatesInSpec($storename, $specId)
    {
        if(!isset($this->specPredicates[$storename]))
        {
            $this->specPredicates[$storename] = $this->getDefinedPredicatesInSpecs($storename);
        }
        if(isset($this->specPredicates[$storename][$specId]))
        {
            return $this->specPredicates[$storename][$specId];
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
     * @internal param string $specName
     * @return Array|null
     */
    public static function getConfig()
    {
        if(isset(self::$config))
        {
            return self::$config;
        }
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
     * @throws MongoTripodConfigException
     * @internal param string $specName
     * @return MongoTripodConfig
     */
    public static function getInstance()
    {
        if (!isset(self::$config))
        {
            throw new MongoTripodConfigException("Call MongoTripodConfig::setConfig() first");
        }
        if (!isset(self::$instance))
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
        foreach ($this->getTableSpecifications($dbName) as $tspec)
        {
            if (array_key_exists("ensureIndexes",$tspec))
            {
                // Indexes should be keyed by data_source
                if(!isset($tableIndexes[$tspec['to']]))
                {
                    $tableIndexes[$tspec['to']] = array();
                }
                foreach ($tspec["ensureIndexes"] as $index)
                {
                    $tableIndexes[$tspec['to']][] = $index;
                }
            }
        }
        $indexes[TABLE_ROWS_COLLECTION] = $tableIndexes;

        $viewIndexes = array();
        foreach ($this->getViewSpecifications($dbName) as $vspec)
        {
            if (array_key_exists("ensureIndexes",$vspec))
            {
                // Indexes should be keyed by data_source
                if(!isset($viewIndexes[$vspec['to']]))
                {
                    $viewIndexes[$vspec['to']] = array();
                }
                foreach ($vspec["ensureIndexes"] as $index)
                {
                    $viewIndexes[$vspec['to']][] = $index;
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
     * @param string $storeName
     * @param string $pod
     * @return bool
     */
    public function isPodWithinStore($storeName,$pod)
    {
        return (array_key_exists($storeName,$this->podConnections)) ? array_key_exists($pod,$this->podConnections[$storeName]) : false;
    }

    /**
     * Returns an array of collection configurations for the supplied database name
     * @param string $storeName
     * @return array
     */
    public function getPods($storeName)
    {
        return (array_key_exists($storeName,$this->podConnections)) ? array_keys($this->podConnections[$storeName]) : array();
    }

    /**
     * Returns the name of the data source for the request pod.  This may be the default for the store or the pod may
     * have overridden it in the config.
     *
     * @param string $storeName
     * @param string $podName
     * @throws MongoTripodConfigException
     * @return string
     */
    public function getDataSourceForPod($storeName, $podName)
    {
        if(isset($this->podConnections[$storeName]) && isset($this->podConnections[$storeName][$podName]))
        {
            return $this->podConnections[$storeName][$podName];
        }
        throw new MongoTripodConfigException("'{$podName}' not configured for store '{$storeName}'");
    }

    /**
     * Returns the connection string for the supplied database name
     *
     * @param string $storeName
     * @param string|null $podName
     * @throws MongoTripodConfigException
     * @return string
     */
    public function getConnStr($storeName, $podName = null)
    {
        if (array_key_exists($storeName,$this->dbConfig))
        {
            if(!$podName)
            {
                return $this->getConnStrForDataSource($this->dbConfig[$storeName]['data_source']);
            }
            $pods = $this->getPods($storeName);
            if(array_key_exists($podName, $pods))
            {
                return $this->getConnStrForDataSource($pods[$podName]['data_source']);
            }
            throw new MongoTripodConfigException("Collection $podName does not exist for database $storeName");
        }
        else
        {
            throw new MongoTripodConfigException("Database $storeName does not exist in configuration");
        }
    }

    /**
     * Returns the transaction log database connection string
     * @return string
     * @throws MongoTripodConfigException
     */
    public function getTransactionLogConnStr() {
        return $this->getConnStrForDataSource($this->tConfig['data_source']);
    }

    /**
     * @param $dataSource
     * @return string
     * @throws MongoTripodConfigException
     */
    protected function getConnStrForDataSource($dataSource)
    {
        if(!array_key_exists($dataSource, $this->dataSources))
        {
            throw new MongoTripodConfigException("Data source '{$dataSource}' not configured");
        }
        $ds = $this->dataSources[$dataSource];
        if(array_key_exists("replicaSet", $ds) && !empty($ds["replicaSet"])) {
            $connStr = $ds['connection'];
            if ($this->isConnectionStringValidForRepSet($connStr)){
                return $connStr;
            } else {
                throw new MongoTripodConfigException("Connection string for '{$dataSource}' must include /admin database when connecting to Replica Set");
            }
        } else {
            return $ds['connection'];
        }
    }

    /**
     * Returns the queue database connection string
     *
     * @return string
     * @throws MongoTripodConfigException
     */
    public function getQueueConnStr() {
        return $this->getConnStrForDataSource($this->queueConfig['data_source']);
    }

    /**
     * Returns a replica set name for the database, if one has been defined
     * @param $datasource
     * @internal param string $dbName
     * @return string|null
     */
    public function getReplicaSetName($datasource)
    {
        if($this->isReplicaSet($datasource))
        {
            return $this->dataSources[$datasource]['replicaSet'];
        }

        return null;
    }

    /**
     * Returns a boolean reflecting whether or not a replica set has been defined for the supplied database name
     * @param $datasource
     * @internal param string $dbName
     * @return bool
     */
    public function isReplicaSet($datasource)
    {
        if (array_key_exists($datasource,$this->dataSources))
        {
            if(array_key_exists("replicaSet",$this->dataSources[$datasource]) && !empty($this->dataSources[$datasource]["replicaSet"])) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param string $storeName
     * @return null
     */
    public function getDefaultDataSourceForStore($storeName)
    {
        if(array_key_exists($storeName, $this->dbConfig))
        {
            return $this->dbConfig[$storeName]['data_source'];
        }
        return null;
    }

    /**
     * Return the view specification document for the supplied id, if it exists
     * @param string $storeName
     * @param string $vid
     * @return array|null
     */
    public function getViewSpecification($storeName, $vid)
    {
        if (isset($this->viewSpecs[$storeName]) && isset($this->viewSpecs[$storeName][$vid]))
        {
            return $this->viewSpecs[$storeName][$vid];
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the search document specification for the supplied id, if it exists
     * @param string $storeName
     * @param string $sid
     * @return array|null
     */
    public function getSearchDocumentSpecification($storeName, $sid)
    {
        if (array_key_exists($storeName, $this->searchDocSpecs) && array_key_exists($sid, $this->searchDocSpecs[$storeName]))
        {
            return $this->searchDocSpecs[$storeName][$sid];
        }

        return null;
    }

    /**
     * Returns an array of all search document specifications, or specification ids
     *
     * @param string $storeName
     * @param string|null $type When supplied, will only return search document specifications that are triggered by this rdf:type
     * @param bool $justReturnSpecId default is false. If true will only return an array of specification _id's, otherwise returns the array of specification documents
     * @return array
     */
    public function getSearchDocumentSpecifications($storeName, $type=null, $justReturnSpecId=false)
    {

        if(!isset($this->searchDocSpecs[$storeName]) || empty($this->searchDocSpecs[$storeName]))
        {
            return array();
        }
        $specs = array();

        if(empty($type)){
            if($justReturnSpecId){
                $specIds = array();
                foreach($this->searchDocSpecs[$storeName] as $specId=>$spec){
                    $specIds[] = $specId;
                }
                return $specIds;
            } else {
                return $this->searchDocSpecs[$storeName];
            }
        }

        $labeller = $this->getLabeller();
        $typeAsUri = $labeller->uri_to_alias($type);
        $typeAsQName = $labeller->qname_to_alias($type);

        foreach ($this->searchDocSpecs[$storeName] as $spec)
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
     * @param string $storeName
     * @param string $tid
     * @return array|null
     */
    public function getTableSpecification($storeName, $tid)
    {
        if (isset($this->tableSpecs[$storeName]) && isset($this->tableSpecs[$storeName][$tid]))
        {
            return $this->tableSpecs[$storeName][$tid];
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns all defined table specifications
     * @codeCoverageIgnore
     * @param string $storeName
     * @return array
     */
    public function getTableSpecifications($storeName)
    {
        return (isset($this->tableSpecs[$storeName]) ? $this->tableSpecs[$storeName] : array());
    }

    /**
     * Returns all defined view specification
     * @codeCoverageIgnore
     * @param string $storeName
     * @return array
     */
    public function getViewSpecifications($storeName)
    {
        return (isset($this->viewSpecs[$storeName])  ? $this->viewSpecs[$storeName] : array());
    }


    /**
     * This method returns a unique list of every rdf type configured in a specifications ['type'] restriction
     * @param string $storeName
     * @return array of types
     */
    public function getAllTypesInSpecifications($storeName)
    {
        $viewTypes   = $this->getTypesInViewSpecifications($storeName);
        $tableTypes  = $this->getTypesInTableSpecifications($storeName);
        $searchTypes = $this->getTypesInSearchSpecifications($storeName);
        $types = array_unique(array_merge($viewTypes, $tableTypes, $searchTypes));
        return array_values($types);
    }

    /**
     * Returns a unique list of every rdf type configured in the view spec ['type'] restriction
     * @param string $storeName
     * @param string|null $pod
     * @return array
     */
    public function getTypesInViewSpecifications($storeName, $pod=null)
    {
        return array_unique($this->getSpecificationTypes($this->getViewSpecifications($storeName), $pod));
    }

    /**
     * Returns a unique list of every rdf type configured in the table spec ['type'] restriction
     * @param string $storeName
     * @param string|null $pod
     * @return array
     */
    public function getTypesInTableSpecifications($storeName, $pod = null)
    {
        return array_unique($this->getSpecificationTypes($this->getTableSpecifications($storeName), $pod));
    }

    /**
     * Returns a unique list of every rdf type configured in the search doc spec ['type'] restriction
     * @param string $storeName
     * @param string|null $pod
     * @return array
     */
    public function getTypesInSearchSpecifications($storeName, $pod = null)
    {
        return array_unique($this->getSpecificationTypes($this->getSearchDocumentSpecifications($storeName), $pod));
    }

    /**
     * Returns an array of database names
     * @todo Refactor this for stores/pods
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
        self::$instance = null;
        self::$config = null;
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
     * @param string|null $podName
     * @return array
     */
    private function getSpecificationTypes(Array $specifications, $podName=null)
    {
        $types = array();
        foreach($specifications as $spec){

            if(!empty($podName)){
                if($spec['from'] !== $podName){
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
     * @param string $storeName
     * @return string|null
     */
    public function getSearchProviderClassName($storeName)
    {
        return (isset($this->searchProviderClassName[$storeName]) ? $this->searchProviderClassName[$storeName] : null);
    }

    /**
     * @param $storeName
     * @param string|null $dataSource
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoDB
     */
    public function getDatabase($storeName, $dataSource = null, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(!isset($this->dbConfig[$storeName]))
        {
            throw new MongoTripodConfigException("Store name '{$storeName}' not in configuration");
        }

        if(!$dataSource)
        {
            $dataSource = $this->dbConfig[$storeName]["data_source"];
        }

        if(!isset($this->dataSources[$dataSource]))
        {
            throw new MongoTripodConfigException("Data source '{$dataSource}' not in configuration");
        }
        $connectionOptions = array();
        $ds = $this->dataSources[$dataSource];
        $connectionOptions['connectTimeoutMS'] = (isset($ds['connectTimeoutMS']) ? $ds['connectTimeoutMS'] : 20000);

        if(isset($ds['replicaSet']) && !empty($ds['replicaSet'])) {
            $connectionOptions['replicaSet'] = $ds['replicaSet'];
        }
        $client = new MongoClient($ds['connection'], $connectionOptions);
        $db = $client->selectDB($this->dbConfig[$storeName]['database']);
        $db->setReadPreference($readPreference);
        return $db;
    }

    /**
     * @param MongoDB $db
     * @param string $collectionName
     * @return MongoCollection
     */
    protected function getMongoCollection(MongoDB $db, $collectionName)
    {
        return $db->selectCollection($collectionName);
    }

    /**
     * @param string $storeName
     * @param string $podName
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection
     */
    public function getCollectionForCBD($storeName, $podName, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(isset($this->podConnections[$storeName]) && isset($this->podConnections[$storeName][$podName]))
        {
            return $this->getMongoCollection(
                $this->getDatabase($storeName, $this->podConnections[$storeName][$podName], $readPreference),
                $podName
            );
        }
        throw new MongoTripodConfigException("Collection name '{$podName}' not in configuration for store '{$storeName}'");
    }

    /**
     * @param string $storeName
     * @param string $viewId
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection
     */
    public function getCollectionForView($storeName, $viewId, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(isset($this->viewSpecs[$storeName]) && isset($this->viewSpecs[$storeName][$viewId]))
        {
            return $this->getMongoCollection(
                $this->getDatabase($storeName, $this->viewSpecs[$storeName][$viewId]['to'], $readPreference),
                VIEWS_COLLECTION
            );
        }
        throw new MongoTripodConfigException("View id '{$viewId}' not in configuration for store '{$storeName}'");
    }

    /**
     * @param string $storeName
     * @param string $searchDocumentId
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection
     */
    public function getCollectionForSearchDocument($storeName, $searchDocumentId, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(array_key_exists($storeName, $this->searchDocSpecs) && array_key_exists($searchDocumentId, $this->searchDocSpecs[$storeName]))
        {
            return $this->getMongoCollection(
                $this->getDatabase($storeName, $this->searchDocSpecs[$storeName][$searchDocumentId]['to'], $readPreference),
                SEARCH_INDEX_COLLECTION
            );
        }
        throw new MongoTripodConfigException("Search document id '{$searchDocumentId}' not in configuration for store '{$storeName}'");
    }

    /**
     * @param string $storeName
     * @param string $tableId
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection
     */
    public function getCollectionForTable($storeName, $tableId, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(isset($this->tableSpecs[$storeName][$tableId]) && isset($this->tableSpecs[$storeName][$tableId]))
        {
            return $this->getMongoCollection(
                $this->getDatabase($storeName, $this->tableSpecs[$storeName][$tableId]['to'], $readPreference),
                TABLE_ROWS_COLLECTION
            );
        }
        throw new MongoTripodConfigException("Table id '{$tableId}' not in configuration for store '{$storeName}'");
    }

    /**
     * @param string $storeName
     * @param array $tables
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection[]
     */
    public function getCollectionsForTables($storeName, array $tables = array(), $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(!isset($this->tableSpecs[$storeName]))
        {
            return array();
        }
        if(empty($tables))
        {
            $tables = array_keys($this->tableSpecs[$storeName]);
        }
        $dataSources = array();
        foreach($tables as $table)
        {
            if(isset($this->tableSpecs[$storeName][$table]))
            {
                $dataSources[] = $this->tableSpecs[$storeName][$table]['to'];
            }
            else
            {
                throw new MongoTripodConfigException("Table id '{$table}' not in configuration for store '{$storeName}'");
            }
        }

        $collections = array();
        foreach(array_unique($dataSources) as $dataSource)
        {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                TABLE_ROWS_COLLECTION
            );
        }
        return $collections;
    }

    /**
     * @param string $storeName
     * @param array $views
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection[]
     */
    public function getCollectionsForViews($storeName, array $views = array(), $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(!isset($this->viewSpecs[$storeName]))
        {
            return array();
        }
        if(empty($views))
        {
            $views = array_keys($this->viewSpecs[$storeName]);
        }
        $dataSources = array();
        foreach($views as $view)
        {
            if(isset($this->viewSpecs[$storeName][$view]))
            {
                $dataSources[] = $this->viewSpecs[$storeName][$view]['to'];
            }
            else
            {
                throw new MongoTripodConfigException("View id '{$view}' not in configuration for store '{$storeName}'");
            }
        }

        $collections = array();
        foreach(array_unique($dataSources) as $dataSource)
        {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                VIEWS_COLLECTION
            );
        }
        return $collections;
    }

    /**
     * @param string $storeName
     * @param array $searchSpecIds
     * @param string $readPreference
     * @throws MongoTripodConfigException
     * @return MongoCollection[]
     */
    public function getCollectionsForSearch($storeName, array $searchSpecIds = array(), $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        if(!isset($this->searchDocSpecs[$storeName]))
        {
            return array();
        }
        if(empty($searchSpecIds))
        {
            $searchSpecIds = array_keys($this->searchDocSpecs[$storeName]);
        }
        $dataSources = array();
        foreach($searchSpecIds as $searchSpec)
        {
            if(isset($this->searchDocSpecs[$storeName][$searchSpec]))
            {
                $dataSources[] = $this->searchDocSpecs[$storeName][$searchSpec]['to'];
            }
            else
            {
                throw new MongoTripodConfigException("Search document spec id '{$searchSpec}' not in configuration for store '{$storeName}'");
            }
        }

        $collections = array();
        foreach(array_unique($dataSources) as $dataSource)
        {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                SEARCH_INDEX_COLLECTION
            );
        }
        return $collections;
    }

    /**
     * @param string $storeName
     * @param string $readPreference
     * @return MongoCollection
     */
    public function getCollectionForTTLCache($storeName, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            TTL_CACHE_COLLECTION
        );
    }

    /**
     * @param string $storeName
     * @param string $readPreference
     * @return MongoCollection
     */
    public function getCollectionForLocks($storeName, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            LOCKS_COLLECTION
        );
    }

    /**
     * @param string $storeName
     * @param string $readPreference
     * @return MongoCollection
     */
    public function getCollectionForManualRollbackAudit($storeName, $readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            AUDIT_MANUAL_ROLLBACKS_COLLECTION
        );
    }

    /**
     * @param $readPreference
     * @return MongoDB
     * @throws MongoTripodConfigException
     */
    public function getQueueDatabase($readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {

        if(!isset($this->dataSources[$this->queueConfig['data_source']]))
        {
            throw new MongoTripodConfigException("Data source '" . $this->queueConfig['data_source'] . "' not in configuration");
        }
        $connectionOptions = array();
        $dataSource = $this->dataSources[$this->queueConfig['data_source']];
        if(isset($dataSource['connectTimeoutMS']) ? $dataSource['connectTimeoutMS'] : 20000);

        if(isset($dataSource['replicaSet']) && !empty($dataSource['replicaSet'])) {
            $connectionOptions['replicaSet'] = $dataSource['replicaSet'];
        }
        $client = new MongoClient($dataSource['connection'], $connectionOptions);
        $db = $client->selectDB($this->queueConfig['database']);
        $db->setReadPreference($readPreference);
        return $db;
    }

    /**
     * @param $readPreference
     * @return MongoDB
     * @throws MongoTripodConfigException
     */
    public function getTransactionLogDatabase($readPreference = MongoClient::RP_PRIMARY_PREFERRED)
    {

        if(!isset($this->dataSources[$this->tConfig['data_source']]))
        {
            throw new MongoTripodConfigException("Data source '" . $this->tConfig['data_source'] . "' not in configuration");
        }
        $connectionOptions = array();
        $dataSource = $this->dataSources[$this->tConfig['data_source']];
        if(isset($dataSource['connectTimeoutMS']) ? $dataSource['connectTimeoutMS'] : 20000);

        if(isset($dataSource['replicaSet']) && !empty($dataSource['replicaSet'])) {
            $connectionOptions['replicaSet'] = $dataSource['replicaSet'];
        }
        $client = new MongoClient($dataSource['connection'], $connectionOptions);
        $db = $client->selectDB($this->queueConfig['database']);
        $db->setReadPreference($readPreference);
        return $db;
    }
}
class MongoTripodConfigException extends Exception {}