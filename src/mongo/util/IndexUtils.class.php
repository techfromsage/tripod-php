<?php
require_once(TRIPOD_DIR."mongo/MongoTripodConfig.class.php");

/**
 * Class IndexUtils
 */
class IndexUtils
{
    /**
     * Ensures the index for the given $storeName. As a consequence, sets the global
     * MongoCursor timeout to -1 for this thread, so use with caution from anything
     * other than a setup script
     * @param bool $reindex - force a reindex of existing data
     * @param null $storeName - database name to ensure indexes for
     * @param bool $background - index in the background (default) or lock DB whilst indexing
     */
    public function ensureIndexes($reindex=false,$storeName=null,$background=true)
    {
        //MongoCursor::$timeout = -1; // set this otherwise you'll see timeout errors for large indexes
        $config = MongoTripodConfig::getInstance();
        $dbs = ($storeName==null) ? $config->getDbs() : array($storeName);
        foreach ($dbs as $storeName)
        {
            $collections = MongoTripodConfig::getInstance()->getIndexesGroupedByCollection($storeName);
            foreach ($collections as $collectionName=>$indexes)
            {
                // Don't do this for composites, which could be anywhere
                if(in_array($collectionName, array(TABLE_ROWS_COLLECTION,VIEWS_COLLECTION,SEARCH_INDEX_COLLECTION)))
                {
                    continue;
                }
                if ($reindex)
                {
                    $config->getCollectionForCBD($storeName, $collectionName)->deleteIndexes();
                }
                foreach ($indexes as $indexName=>$fields)
                {
                    $indexName = substr($indexName,0,127); // ensure max 128 chars
                    if (is_numeric($indexName))
                    {
                        // no name
                        $config->getCollectionForCBD($storeName, $collectionName)->ensureIndex($fields,array("background"=>$background));
                    }
                    else
                    {
                        $config->getCollectionForCBD($storeName, $collectionName)->ensureIndex($fields,array('name'=>$indexName,"background"=>$background));
                    }
                }
            }

            // Index views
            foreach($config->getViewSpecifications($storeName) as $viewId=>$spec)
            {
                $collection = MongoTripodConfig::getInstance()->getCollectionForView($storeName, $viewId);
                if($collection)
                {
                    $indexes = array("_id.type"=>1);
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        $collection->deleteIndexes();
                    }
                    $collection->ensureIndex($indexes, array("background"=>$background));
                }
            }

            // Index table rows
            foreach($config->getTableSpecifications($storeName) as $tableId=>$spec)
            {
                $collection = MongoTripodConfig::getInstance()->getCollectionForTable($storeName, $tableId);
                if($collection)
                {
                    $indexes = array("_id.type"=>1);
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        $collection->deleteIndexes();
                    }
                    $collection->ensureIndex($indexes, array("background"=>$background));
                }
            }
        }
    }
}
