<?php
interface ITripod
{
    public function cbds($collection);

    public function views($viewType);

    public function tables($tableType);

    public function search($searchType);
//    // graph functions
//
//    public function graph($query,$includeProperties=array());
//
//    /**
//     * @deprecated
//     * @abstract
//     * @param $query
//     * @return mixed
//     */
//    public function describe($query);
//    public function describeResource($resource,$context=null);
//    public function describeResources(Array $resources,$context=null);
//
//    public function getViewForResource($resource,$viewType);
//    public function getViewForResources(Array $resources,$viewType);
//    public function getViews(Array $filter,$viewType);
//
//    public function getETag($resource,$context=null);
//
//    // tabular functions
//
//    public function select($query,$fields,$sortBy=null,$limit=null,$context=null);
//
//    public function getTableRows($tableType,$filter=array(),$sortBy=array(),$offset=0,$limit=10);
//    public function generateTableRows($tableType,$resource=null,$context=null);
//
//    public function getDistinctTableColumnValues($tableType, $fieldName, array $filter = array());
//
//    // aggregate, save and search functions
//
//    public function getCount($query,$groupBy=null);
//
//    public function saveChanges(ExtendedGraph $oldGraph, ExtendedGraph $newGraph,$context=null,$description=null);
//
//    public function search(Array $params);
//
//    public function getLockedDocuments($fromDateTime =null , $tillDateTime = null);
//
//    public function removeInertLocks($transaction_id, $reason);
}