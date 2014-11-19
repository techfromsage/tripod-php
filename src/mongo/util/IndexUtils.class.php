<?php
require_once(TRIPOD_DIR."mongo/MongoTripodConfig.class.php");

class IndexUtils
{
    /**
     * Ensures the index for the given $dbName. As a consequence, sets the global
     * MongoCursor timeout to -1 for this thread, so use with caution from anything
     * other than a setup script
     * @param bool $reindex - force a reindex of existing data
     * @param string $configSpec
     * @param string|null $dbName - database name to ensure indexes for
     * @param bool $background - index in the background (default) or lock DB whilst indexing
     */
    public function ensureIndexes($reindex=false, $configSpec=MongoTripodConfig::DEFAULT_CONFIG_SPEC ,$dbName = null, $background=true)
    {
        //MongoCursor::$timeout = -1; // set this otherwise you'll see timeout errors for large indexes

        $config = MongoTripodConfig::getInstance($configSpec);
        $dbs = ($dbName==null) ? $config->getDbs() : array($dbName);
        foreach ($dbs as $dbName)
        {
            $db = $config->getDatabase($dbName);
            $collections = $config->getIndexesGroupedByCollection($dbName);
            foreach ($collections as $collectionName=>$indexes)
            {
                if ($reindex)
                {

                    $db->selectCollection($collectionName)->deleteIndexes();
                }
                foreach ($indexes as $indexName=>$fields)
                {
                    $indexName = substr($indexName,0,127); // ensure max 128 chars
                    if (is_numeric($indexName))
                    {
                        // no name
                        $db->selectCollection($collectionName)->ensureIndex($fields,array("background"=>$background));
                    }
                    else
                    {
                        $db->selectCollection($collectionName)->ensureIndex($fields,array('name'=>$indexName,"background"=>$background));
                    }
                }
            }
            // finally, if views collection is defined for this DB, make sure type is indexed
            if(isset($collections[VIEWS_COLLECTION]))
            {
                $db->selectCollection(VIEWS_COLLECTION)->ensureIndex(array("_id.type"=>1),array("background"=>$background));
            }
        }
    }
}
