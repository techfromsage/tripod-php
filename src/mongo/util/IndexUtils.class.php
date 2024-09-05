<?php

namespace Tripod\Mongo;

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
        $reindexedCollections = [];
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
                    $collection = $config->getCollectionForCBD($storeName, $collectionName);
                    if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                        $collection->dropIndexes();
                        $reindexedCollections[] = $collection->getNamespace();
                    }
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
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1, _ID_KEY.'.'._ID_TYPE => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        ['value.'._IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
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
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1, _ID_KEY.'.'._ID_TYPE => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        ['value.'._IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];
                    if(isset($spec['ensureIndexes']))
                    {
                        $indexes = array_merge($indexes, $spec['ensureIndexes']);
                    }
                    if ($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
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
                    $indexes = [
                        [_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1],
                        [_ID_KEY.'.'._ID_TYPE => 1],
                        [_IMPACT_INDEX => 1],
                        [\_CREATED_TS => 1]
                    ];

                    if($reindex)
                    {
                        if (!in_array($collection->getNamespace(), $reindexedCollections)) {
                            $collection->dropIndexes();
                            $reindexedCollections[] = $collection->getNamespace();
                        }
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

    /**
     * returns mongo tripod config instance, this method aids helps with
     * testing.
     * @return \Tripod\Mongo\Config
     */
    protected function getConfig()
    {
        return \Tripod\Config::getInstance();
    }
}
