<?php

namespace Tripod\Mongo;

use \MongoDB\Driver\ReadPreference;
use \MongoDB\Collection;

/**
 * Class SearchDocuments
 * @package Tripod\Mongo
 */
class SearchDocuments extends DriverBase
{
    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there
     * @param string $storeName
     * @param Collection $collection
     * @param string $defaultContext
     * @param \Tripod\ITripodStat|null $stat
     * @param string $readPreference
     */
    public function __construct($storeName, Collection $collection, $defaultContext, $stat=null , $readPreference = ReadPreference::RP_PRIMARY)
    {
        $this->labeller = new Labeller();
        $this->storeName = $storeName;
        $this->collection = $collection;
        $this->podName = $collection->getCollectionName();
        $this->defaultContext = $defaultContext;
        $this->stat = $stat;
        $this->readPreference = $readPreference;
    }

    /**
     * @param string $specId
     * @param string $resource
     * @param string $context
     * @return array|null
     * @throws \Exception
     */
    public function generateSearchDocumentBasedOnSpecId($specId, $resource, $context)
    {
        if (empty($resource))
        {
            throw new \Exception("Resource must be specified");
        }
        if (empty($context))
        {
            throw new \Exception("Context must be specified");
        }

        $searchSpec = $this->getSearchDocumentSpecification($specId);
        if(empty($searchSpec)){
            $this->debugLog("Could not find Search Document Specification for $specId");
            return null;
        }

        if (isset($searchSpec["from"]))
        {
            $from = $searchSpec["from"];
        }
        else
        {
            $from = $this->podName;
        }

        // work out whether or not to index at all
        $proceedWithGeneration = false;

        foreach ($searchSpec['filter'] as $indexRules)
        {
            // run a query to work out
            if (!empty($indexRules['condition']))
            {
                $irFrom = (!empty($indexRules['from'])) ? $indexRules['from'] : $this->podName;
                // add id of current record to rules..
                $indexRules['condition']['_id'] = array(
                    'r'=>$this->labeller->uri_to_alias($resource),
                    'c'=>$this->labeller->uri_to_alias($context));

                if (Config::getInstance()->getCollectionForCBD($this->storeName, $irFrom)->findOne($indexRules['condition']))
                {
                    // match found, add this spec id to those that should be generated
                   $proceedWithGeneration = true;
                }
            }
            else
            {
                // no restriction rules, so just add to generate
                $proceedWithGeneration = true;
            }
        }

        if($proceedWithGeneration == false){
            $this->debugLog("Unable to proceed with generating $specId search document for $resource, does not satisfy rules");
            return null;
        }

        $_id = array(
            'r'=>$this->labeller->uri_to_alias($resource),
            'c'=>$this->labeller->uri_to_alias($context)
        );

        $sourceDocument = Config::getInstance()->getCollectionForCBD($this->storeName, $from)->findOne(array('_id'=>$_id));

        if(empty($sourceDocument)){
            $this->debugLog("Source document not found for $resource, cannot proceed generating $specId search document");
            return null;
        }

        $this->debugLog("Processing {$specId}");

        // build the document
        $generatedDocument = [\_CREATED_TS => DateUtil::getMongoDate()];
        $this->addIdToImpactIndex($_id, $generatedDocument);

        $_id['type'] = $specId;
        $generatedDocument['_id'] = $_id;

        if(isset($searchSpec['fields'])){
            $this->addFields($sourceDocument, $searchSpec['fields'], $generatedDocument);
        }
        if(isset($searchSpec['indices'])){
            $this->addFields($sourceDocument, $searchSpec['indices'], $generatedDocument, true);
        }
        if(isset($searchSpec['joins'])){
            $this->doJoin($sourceDocument, $searchSpec['joins'], $generatedDocument, $from);
        }

        return $generatedDocument;
    }

    /**
     * @param array $rdfTypes
     * @param string $resource
     * @param string $context
     * @return array
     * @throws \Exception
     */
    public function generateSearchDocumentsBasedOnRdfTypes(Array $rdfTypes, $resource, $context)
    {
        if (empty($resource))
        {
            throw new \Exception("Resource must be specified");
        }
        if (empty($context))
        {
            throw new \Exception("Context must be specified");
        }

        // this is what is returned
        $generatedSearchDocuments = array();

        $timer =new \Tripod\Timer();
        $timer->start();

        foreach($rdfTypes as $rdfType)
        {
            $specs = Config::getInstance()->getSearchDocumentSpecifications($this->storeName, $rdfType);

            if(empty($specs)) continue; // no point doing anything else if there is no spec for the type

            foreach($specs as $searchSpec)
            {
                $generatedSearchDocuments[] = $this->generateSearchDocumentBasedOnSpecId($searchSpec['_id'], $resource, $context);
            }
        }
        $timer->stop();
        //echo "\n\tTook " . $timer->result() . " ms to generate search documents\n";
        return $generatedSearchDocuments;
    }

    /**
     * @param array $source
     * @param array $joins
     * @param array $target
     * @param string $from
     */
    protected function doJoin($source, $joins, &$target, $from)
    {
        // expand sequences before proceeding
        $this->expandSequence($joins, $source);
        $config = Config::getInstance();
        foreach($joins as $predicate=>$rules){
            if(isset($source[$predicate])){
                $joinUris = array();

                if(isset($source[$predicate]['u'])){
                    //single value for join
                    $joinUris[] = array('r'=>$source[$predicate]['u'],'c'=>$this->defaultContext); // todo: check that default context is the right thing to set here and below
                } else {
                    //multiple values for join
                    foreach($source[$predicate] as $v){
                        $joinUris[] = array('r'=>$v['u'],'c'=>$this->defaultContext);
                    }
                }

                $recursiveJoins = array();

                $collection = (isset($rules['from'])
                    ? $config->getCollectionForCBD($this->storeName, $rules['from'])
                    : $config->getCollectionForCBD($this->storeName, $from)
                );

                $cursor = $collection->find(array('_id'=>array('$in'=>$joinUris)), array(
                    'maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout()
                ));

                // add to impact index
                $this->addIdToImpactIndex($joinUris, $target);
                foreach($cursor as $linkMatch)
                {
                    if(isset($rules['fields'])){
                        $this->addFields($linkMatch, $rules['fields'], $target);
                    }

                    if(isset($rules['indices'])){
                        $this->addFields($linkMatch, $rules['indices'], $target, true);
                    }

                    if(isset($rules['join'])){
                        $recursiveJoins[] = array('data'=> $linkMatch, 'ruleset'=> $rules['joins']);
                    }
                }

                foreach($recursiveJoins as $rj){
                    $this->doJoin($rj['data'], $rj['ruleset'], $target, $from);
                }

            }
        }
    }

    /**
     * @param array $source
     * @param array $fieldsOrIndices
     * @param array $target
     * @param bool $isIndex
     */
    protected function addFields(Array $source, Array $fieldsOrIndices, Array &$target, $isIndex=false)
    {
        foreach($fieldsOrIndices as $f){

            if(isset($f['predicates'])){
                $predicates = $f['predicates'];
                foreach($predicates as $p){

                    $values = array();

                    if(isset($source[$p])){
                        if(isset($source[$p]['u'])){
                            $values[] = ($isIndex) ? mb_strtolower(trim($source[$p]['u']), 'UTF-8') : trim($source[$p]['u']);
                        } else if (isset($source[$p]['l'])){
                            $values[] = ($isIndex) ? mb_strtolower(trim($source[$p]['l']), 'UTF-8') : trim($source[$p]['l']);
                        } else {
                            foreach($source[$p] as $v){
                                if(isset($v['u'])){
                                    $values[] = ($isIndex) ? mb_strtolower(trim($v['u']), 'UTF-8') : trim($v['u']);
                                } else {
                                    $values[] = ($isIndex) ? mb_strtolower(trim($v['l']), 'UTF-8') : trim($v['l']);
                                }
                            }
                        }
                    }
                    // now add the values
                    $this->addValuesToTarget($values, $f, $target);
                }
            }

            if(isset($f['value'])){
                $values = array();

                if($f['value'] == '_link_' || $f['value'] == 'link'){
                    if($f['value'] == '_link_')
                    {
                        $this->warningLog("Search spec value '_link_' is deprecated", $f);
                    }
                    $values[] = $this->labeller->qname_to_alias($source['_id']['r']);
                }

                $this->addValuesToTarget($values, $f, $target);
            }
        }
    }

    /**
     * @param $specId
     * @return array|null
     */
    protected function getSearchDocumentSpecification($specId)
    {
        return Config::getInstance()->getSearchDocumentSpecification($this->storeName, $specId);
    }

    /**
     * @param array $values
     * @param array $field
     * @param array $target
     */
    private function addValuesToTarget($values, $field, &$target)
    {
        $objName = null;
        $name  = $field['fieldName'];

        if(strpos($name, '.') !== FALSE){
            $parts = explode('.', $name);
            $objName = $parts[0];
            $name = $parts[1];
        }  // todo: if theres more than 2 parts throw error

        $limit = null;
        if(isset($field['limit'])){
            $limit = $field['limit'];
        } else {
            $limit = count($values);
        }

        if(count($values) > 0){
            for ($i=0; $i<$limit; $i++) {
                $v = $values[$i];
                if (empty($objName)) {
                    if (!isset($target[$name])) {
                        $target[$name] = $v;
                    } else if (is_array($target[$name])) {
                        $target[$name][] = $v;
                    } else {
                        $existingVal = $target[$name];
                        $target[$name] = array();
                        $target[$name][] = $existingVal;
                        $target[$name][] = $v;
                    }
                } else {
                    if (!isset($target[$objName][$name])) {
                        $target[$objName][$name] = $v;
                    } else if (is_array($target[$objName][$name])) {
                        $target[$objName][$name][] = $v;
                    } else {
                        $existingVal = $target[$objName][$name];
                        $target[$objName][$name] = array();
                        $target[$objName][$name][] = $existingVal;
                        $target[$objName][$name][] = $v;
                    }
                }
            }
        }
    }

    /**
     * @return string
     */
    public function getSearchCollectionName()
    {
        return SEARCH_INDEX_COLLECTION;
    }
}
