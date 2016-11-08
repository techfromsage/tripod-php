<?php

namespace Tripod\Mongo;

require_once(TRIPOD_DIR . "mongo/Config.class.php");

/**
 * Class IndexUtils
 * @package Tripod\Mongo
 */
class IndexUtils
{
    /**
     * Ensures the index for the given $storeName.
     * @param bool $reindex - force a reindex of existing data
     * @param null $storeName - database name to ensure indexes for
     * @param bool $background - index in the background (default) or lock DB whilst indexing
     */
    public function ensureIndexes($reindex=false,$storeName=null,$background=true)
    {
        $config = $this->getConfig();
        $dbs = ($storeName==null) ? $config->getDbs() : array($storeName);
        foreach ($dbs as $storeName)
        {
            $collections = $config->getIndexesGroupedByCollection($storeName);
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
                        $config->getCollectionForCBD($storeName, $collectionName)
                            ->createIndex(
                                $fields,
                                array(
                                    "background"=>$background
                                )
                            );
                    }
                    else
                    {
                        $config->getCollectionForCBD($storeName, $collectionName)
                            ->createIndex(
                                $fields,
                                array(
                                    'name'=>$indexName,
                                    "background"=>$background
                                )
                            );
                    }
                }
            }

            // Index views
            foreach($config->getViewSpecifications($storeName) as $viewId=>$spec)
            {
                $collection = $config->getCollectionForView($storeName, $viewId);
                if($collection)
                {
                    $indexes = array(
                        array("_id.r" =>1, "_id.c"=>1, "_id.type"=>1),
                        array("_id.type"=>1),
                        array("value._impactIndex"=>1),
                    );
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        $collection->deleteIndexes();
                    }
                    foreach($indexes as $index)
                    {
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }

            // Index table rows
            foreach($config->getTableSpecifications($storeName) as $tableId=>$spec)
            {
                $collection = $config->getCollectionForTable($storeName, $tableId);
                if($collection)
                {
                    $indexes = array(
                        array("_id.r" =>1, "_id.c"=>1, "_id.type"=>1),
                        array("_id.type"=>1),
                        array("value._impactIndex"=>1),
                    );
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        $collection->deleteIndexes();
                    }
                    foreach($indexes as $index)
                    {
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }

            // index search documents
            foreach($config->getSearchDocumentSpecifications($storeName) as $searchId=>$spec)
            {
                $collection = $config->getCollectionForSearchDocument($storeName, $searchId);
                if($collection)
                {
                    $indexes = array(
                        array("_id.r"=>1, "_id.c"=>1),
                        array("_id.type"=>1),
                        array("_impactIndex"=>1)
                    );

                    if($reindex)
                    {
                        $collection->deleteIndexes();
                    }
                    foreach($indexes as $index)
                    {
                        $collection->createIndex(
                            $index,
                            array(
                                "background"=>$background
                            )
                        );
                    }
                }
            }
        }
    }

    protected function getConfig()
    {
        return Config::getInstance();
    }
}
