<?php
/**
 * Holds the global configuration for Tripod
 */
class MongoTripodConfig
{
    private static $instance = null;
    private static $config = null;
    private $labeller = null;

    private $defaultContext = null;
    private $indexes = array();
    private $cardinality = array();
    private $dbConfig = array();
    private $viewSpecs = array();
    private $tableSpecs = array();
    private $databases = array();

    public $ns = array();
    public $dbs = array();
    public $tConfig = array();
    public $queue = array();
    public $searchDocSpecs = array();
    public $searchProvider = null;


    public function __construct(Array $config)
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
        if(array_key_exists("replicaSet", $transactionConfig) && !empty($transactionConfig["replicaSet"])) {
            $this->tConfig['replicaSet'] = $transactionConfig["replicaSet"];
        }
        if(array_key_exists("type", $transactionConfig) && !empty($transactionConfig["type"])) {
            switch ($transactionConfig["type"]) {
                case "MongoTransactionLog":
                case "DoctrineTransactionLog":
                    $this->tConfig['type'] = $transactionConfig["type"];
                    break;
                default:
                    throw new MongoTripodConfigException("Unrecognised transaction log type: ".$transactionConfig["type"]);
                    break;
            }
        } else {
            $this->tConfig['type'] = "MongoTransactionLog";
        }


        $searchConfig = (array_key_exists("search_config",$config)) ? $config["search_config"] : array();
        if(!empty($searchConfig)){
            $this->searchProvider = $this->getMandatoryKey('search_provider', $searchConfig, 'search');

            $searchDocSpecs = $this->getMandatoryKey('search_specifications', $searchConfig, 'search');
            foreach ($searchDocSpecs as $spec)
            {
                $this->searchDocSpecs[$spec["_id"]] = $spec;
            }
        }

        $queueConfig = $this->getMandatoryKey("queue",$config);
        $this->queue["database"] = $this->getMandatoryKey("database",$queueConfig,'queue');
        $this->queue["collection"] = $this->getMandatoryKey("collection",$queueConfig,'queue');
        $this->queue["connStr"] = $this->getMandatoryKey("connStr",$queueConfig,'queue');
        if(array_key_exists("replicaSet", $queueConfig) && !empty($queueConfig["replicaSet"])) {
            $this->queue['replicaSet'] = $queueConfig["replicaSet"];
        }


        $viewSpecs = (array_key_exists("view_specifications",$config)) ? $config["view_specifications"] : array();
        foreach ($viewSpecs as $spec)
        {
            $this->ifCountExistsWithoutTTLThrowException($spec);
            $this->viewSpecs[$spec["_id"]] = $spec;
        }

        $tableSpecs = (array_key_exists("table_specifications",$config)) ? $config["table_specifications"] : array();
        foreach ($tableSpecs as $spec)
        {
            $this->ifCountExistsWithoutTTLThrowException($spec);
            $this->tableSpecs[$spec["_id"]] = $spec;
        }

        $this->databases = $this->getMandatoryKey("databases",$config);
        foreach ($this->databases as $dbName=>$db)
        {
            $this->dbs[] = $dbName;
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
     * @codeCoverageIgnore
     * @static
     * @return MongoTripodConfig
     */
    public static function getConfig()
    {
        return self::$config;
    }

    public function getDefaultContextAlias()
    {
        return $this->getLabeller()->uri_to_alias($this->defaultContext);
    }

    /**
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
            self::$instance = new MongoTripodConfig(self::$config);
        }
        return self::$instance;
    }

    /**
     * set the config
     * @param array $config
     */
    public static function setConfig(Array $config)
    {
        self::$config = $config;
        self::$instance = null; // this will force a reload next time getInstance() is called
    }

    /**
     * Returns a list of the configured indexes grouped by collection
     * @param $dbName
     * @return mixed
     */
    public function getIndexesGroupedByCollection($dbName)
    {
        $indexes = $this->indexes[$dbName];
        //TODO: if we have much more default indexes we should find a better way of doing this
        foreach($indexes as $collection=>$indices) {
            $indexes[$collection][_LOCKED_FOR_TRANS_INDEX] = array("_id"=>1, _LOCKED_FOR_TRANS=>1);
            $indexes[$collection][_UPDATED_TS_INDEX] = array("_id"=>1, _UPDATED_TS=>1);
            $indexes[$collection][_CREATED_TS_INDEX] = array("_id"=>1, _CREATED_TS=>1);
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
     * @param $dbName String The database name to use.
     * @param $collName String The collection in the database.
     * @param $qName String Either the qname to get the values for or empty for all cardinality values.
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

    public function isCollectionWithinConfig($dbName,$collName)
    {
        return (array_key_exists($dbName,$this->databases)) ? array_key_exists($collName,$this->databases[$dbName]['collections']) : false;
    }

    public function getCollections($dbName)
    {
        return (array_key_exists($dbName,$this->databases)) ? $this->databases[$dbName]['collections'] : array();
    }

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

    public function getTransactionLogType() {
        return $this->tConfig['type'];
    }

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

    public function getQueueConnStr() {
        if(array_key_exists("replicaSet", $this->queue) && !empty($this->queue["replicaSet"])) {
            $connStr = $this->queue['connStr'];
            if ($this->isConnectionStringValidForRepSet($connStr)){
                return $connStr;
            } else {
                throw new MongoTripodConfigException("Connection string for Queue must include /admin database when connecting to Replica Set");
            }
        } else {
            return $this->queue['connStr'];
        }
    }

    public function getReplicaSetName($dbName)
    {
        if($this->isReplicaSet($dbName))
        {
            return $this->dbConfig[$dbName]['replicaSet'];
        }

        return null;
    }

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

    public function getSearchDocumentSpecification($sid)
    {
        if(array_key_exists($sid, $this->searchDocSpecs)) {
            return $this->searchDocSpecs[$sid];
        }

        return null;
    }

    /**
     * @param null $type
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
                    $specIds[] = $spec['_id'];
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
            if(is_array($spec['type'])){
                if(in_array($typeAsUri, $spec['type']) || in_array($typeAsQName, $spec['type'])){
                    if($justReturnSpecId){
                        $specs[] = $spec['_id'];
                    } else {
                        $specs[] = $spec;
                    }
                }
            } else {
                if ($spec["type"]==$typeAsUri || $spec["type"]==$typeAsQName)
                {
                    if($justReturnSpecId){
                        $specs[] = $spec['_id'];
                    } else {
                        $specs[] = $spec;
                    }
                }
            }
        }
        return $specs;
    }


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
     * @codeCoverageIgnore
     * @return array
     */
    public function getTableSpecifications()
    {
        return $this->tableSpecs;
    }

    /**
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

    public function getTypesInViewSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getViewSpecifications(), $collectionName));
    }

    public function getTypesInTableSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getTableSpecifications(), $collectionName));
    }

    public function getTypesInSearchSpecifications($collectionName=null)
    {
        return array_unique($this->getSpecificationTypes($this->getSearchDocumentSpecifications(), $collectionName));
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

    private function getSpecificationTypes(Array $specifications, $collectionName=null)
    {
        $types = array();
        foreach($specifications as $spec){

            if(!empty($collectionName)){
                if($spec['from'] !== $collectionName){
                    continue; // skip this view spec if it isnt for the collection
                }
            }

            if(is_array($spec['type'])){
                $types = array_merge($spec['type'], $types);
            } else {
                $types[] = $spec['type'];
            }
        }
        return $types;
    }

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

    private function getMandatoryKey($key,Array $a,$configName='config')
    {
        if (!array_key_exists($key,$a))
        {
            throw new MongoTripodConfigException("Mandatory config key [$key] is missing from $configName");
        }
        return $a[$key];
    }

    private function isConnectionStringValidForRepSet($connStr)
    {
        $needle = "/admin";
        if (substr($connStr, -6) === $needle){
            return true;
        } else {
            return false;
        }
    }


}
class MongoTripodConfigException extends Exception {}