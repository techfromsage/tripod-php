<?php

namespace Tripod\Mongo\Composites;

require_once TRIPOD_DIR . 'mongo/base/DriverBase.class.php';

use Tripod\Mongo\Config;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Labeller;

/**
 * Class Views
 * @package Tripod\Mongo\Composites
 */
class Views extends CompositeBase
{

    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there
     * @param string $storeName
     * @param \MongoCollection $collection
     * @param $defaultContext
     * @param null $stat
     */
    function __construct($storeName, \MongoCollection $collection,$defaultContext,$stat=null) // todo: $collection -> podname
    {
        $this->storeName = $storeName;
        $this->labeller = new Labeller();
        $this->collection = $collection;
        $this->podName = $collection->getName();
        $this->defaultContext = $defaultContext;
        $this->config = Config::getInstance();
        $this->stat = $stat;
        $this->readPreference = \MongoClient::RP_PRIMARY; // todo: figure out where this should go.
    }

    /**
     * @return string
     */
    public function getOperationType()
    {
        return OP_VIEWS;
    }

    /**
     * Receive update from subject
     * @param \Tripod\Mongo\ImpactedSubject
     * @return void
     */
    public function update(\Tripod\Mongo\ImpactedSubject $subject)
    {
        $resource = $subject->getResourceId();
        $resourceUri    = $resource[_ID_RESOURCE];
        $context        = $resource[_ID_CONTEXT];

        $this->generateViews(array($resourceUri),$context);
    }

    /**
     * @return array
     */
    public function getTypesInSpecifications()
    {
        return $this->config->getTypesInViewSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * @param array $resourcesAndPredicates
     * @param string $contextAlias
     * @return array|mixed
     */
    public function findImpactedComposites(Array $resourcesAndPredicates, $contextAlias)
    {
        $resources = array_keys($resourcesAndPredicates);

        // This should never happen, but in the event that we have been passed an empty array or something
        if(empty($resources))
        {
            return array();
        }

        $contextAlias = $this->getContextAlias($contextAlias); // belt and braces

        // build a filter - will be used for impactIndex detection and finding direct views to re-gen
        $filter = array();
        foreach ($resources as $resource)
        {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            // build $filter for queries to impact index
            $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);
        }

        // first re-gen views where resources appear in the impact index
        $query = array("value."._IMPACT_INDEX=>array('$in'=>$filter));

        $affectedViews = array();
        foreach($this->config->getCollectionsForViews($this->storeName) as $collection)
        {
            $views = $collection->find($query,array("_id"=>true));
            foreach($views as $v)
            {
                $affectedViews[] = $v;
            }
        }
        return $affectedViews;
    }

    /**
     * @param string $storeName
     * @param string $viewSpecId
     * @return array|null
     */
    public function getSpecification($storeName, $viewSpecId)
    {
        return $this->config->getViewSpecification($storeName,$viewSpecId);
    }

    /**
     * Return all views, restricted by $filter conditions, for given $viewType
     * @param array $filter - an array, keyed by predicate, to filter by
     * @param $viewType
     * @return \Tripod\Mongo\MongoGraph
     */
    public function getViews(Array $filter,$viewType)
    {
        $query = array("_id.type"=>$viewType);
        foreach ($filter as $predicate=>$object)
        {
            if (strpos($predicate,'$')===0)
            {
                $values = array();
                foreach ($object as $obj)
                {
                    foreach ($obj as $p=>$o) $values[] = array('value.'._GRAPHS.'.'.$p=>$o);
                }
                $query[$predicate] = $values;
            }
            else
            {
                $query['value.'._GRAPHS.'.'.$predicate] = $object;
            }
        }
        $viewCollection = $this->getConfigInstance()->getCollectionForView($this->storeName, $viewType);
        return $this->fetchGraph($query,MONGO_VIEW,$viewCollection);
    }

    /**
     * For given $resource, return the view of type $viewType
     * @param $resource
     * @param $viewType
     * @param null $context
     * @return \Tripod\Mongo\MongoGraph
     */
    public function getViewForResource($resource,$viewType,$context=null)
    {
        if(empty($resource)){
            return new \Tripod\Mongo\MongoGraph();
        }

        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $query = array( "_id" => array("r"=>$resourceAlias,"c"=>$contextAlias,"type"=>$viewType));
        $viewCollection = $this->config->getCollectionForView($this->storeName, $viewType);
        $graph = $this->fetchGraph($query,MONGO_VIEW,$viewCollection);
        if ($graph->is_empty())
        {
            $this->getStat()->increment(MONGO_VIEW_CACHE_MISS.".$viewType");
            $viewSpec = Config::getInstance()->getViewSpecification($this->storeName, $viewType);
            if($viewSpec == null)
            {
                return new \Tripod\Mongo\MongoGraph();
            }

            $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

            $doc = $this->config->getCollectionForCBD($this->storeName, $fromCollection)
                ->findOne(array( "_id" => array("r"=>$resourceAlias,"c"=>$contextAlias)));

            if($doc == NULL)
            {
                // if you are trying to generate a view for a document that doesnt exist in the collection
                // then we can just return an empty graph
                return new \Tripod\Mongo\MongoGraph();
            }

            // generate view then try again
            $this->generateView($viewType,$resource,$context);
            return $this->fetchGraph($query,MONGO_VIEW,$viewCollection);
        }
        return $graph;
    }

    /**
     * For given $resources, return the views of type $viewType
     * @param array $resources
     * @param $viewType
     * @param null $context
     * @return \Tripod\Mongo\MongoGraph
     */
    public function getViewForResources(Array $resources,$viewType,$context=null)
    {
        $contextAlias = $this->getContextAlias($context);

        $cursorSize = 101;
        if(count($resources) > 101) {
            $cursorSize = count($resources);
        }

        $query = array("_id" => array('$in' => $this->createTripodViewIdsFromResourceUris($resources,$context,$viewType)));
        $g = $this->fetchGraph($query,MONGO_VIEW,$this->getCollectionForViewSpec($viewType), null, $cursorSize);

        // account for missing subjects
        $returnedSubjects = $g->get_subjects();
        $missingSubjects = array_diff($resources,$returnedSubjects);
        if (!empty($missingSubjects))
        {
            $regrabResources = array();
            foreach($missingSubjects as $missingSubject)
            {
                $viewSpec = $this->getConfigInstance()->getViewSpecification($this->storeName, $viewType);
                $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

                $missingSubjectAlias = $this->labeller->uri_to_alias($missingSubject);
                $doc = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $fromCollection)
                    ->findOne(array( "_id" => array("r"=>$missingSubjectAlias,"c"=>$contextAlias)));

                if($doc == NULL)
                {
                    // nothing in source CBD for this subject, there can never be a view for it
                    continue;
                }

                // generate view then try again
                $this->generateView($viewType,$missingSubject,$context);
                $regrabResources[] = $missingSubject;
            }

            if(!empty($regrabResources)) {
                // only try to regrab resources if there are any to regrab
                $cursorSize = 101;
                if(count($regrabResources) > 101) {
                    $cursorSize = count($regrabResources);
                }

                $query = array("_id" => array('$in' => $this->createTripodViewIdsFromResourceUris($regrabResources,$context,$viewType)));
                $g->add_graph($this->fetchGraph($query,MONGO_VIEW,$this->getCollectionForViewSpec($viewType)), null, $cursorSize);
            }
        }

        return $g;
    }

    /**
     * @param string|array $resourceUriOrArray
     * @param string $context
     * @param string $viewType
     * @return array
     */
    private function createTripodViewIdsFromResourceUris($resourceUriOrArray,$context,$viewType)
    {
        $contextAlias = $this->getContextAlias($context);
        $ret = array();
        foreach($resourceUriOrArray as $resource)
        {
            $ret[] = array("r"=>$this->labeller->uri_to_alias($resource),"c"=>$contextAlias,"type"=>$viewType);
        }
        return $ret;
    }

    /**
     * Autodiscovers the multiple view specification that may be applicable for a given resource, and submits each for generation
     * @param $resources
     * @param null $context
     */
    public function generateViews($resources,$context=null)
    {
        $contextAlias = $this->getContextAlias($context);

        // build a filter - will be used for impactIndex detection and finding direct views to re-gen
        $filter = array();
        foreach ($resources as $resource)
        {
            $resourceAlias = $this->labeller->uri_to_alias($resource);

            // delete any views this resource is involved in. It's type may have changed so it's not enough just to regen it with it's new type below.
            foreach (Config::getInstance()->getViewSpecifications($this->storeName) as $type=>$spec)
            {
                if($spec['from']==$this->podName){
                    $this->config->getCollectionForView($this->storeName, $type)
                        ->remove(array("_id" => array("r"=>$resourceAlias,"c"=>$contextAlias,"type"=>$type)));
                }
            }

            // build $filter for queries to impact index
            $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);
        }

        // now generate view for $resources themselves... Maybe an optimisation down the line to cut out the query here
        $query = array("_id"=>array('$in'=>$filter));
        $resourceAndType = $this->collection->find($query,array("_id"=>1,"rdf:type"=>1));

        foreach ($resourceAndType as $rt)
        {
            $id = $rt["_id"];
            if (array_key_exists("rdf:type",$rt))
            {
                if (array_key_exists('u',$rt["rdf:type"]))
                {
                    // single type, not an array of values
                    $this->generateViewsForResourcesOfType($rt["rdf:type"]['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                }
                else
                {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type)
                    {
                        $this->generateViewsForResourcesOfType($type['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                    }
                }
            }
        }
    }

    /**
     * This method finds all the view specs for the given $rdfType and generates the views for the $resource one by one
     * @param $rdfType
     * @param null $resource
     * @param null $context
     * @throws \Exception
     * @return mixed
     */
    public function generateViewsForResourcesOfType($rdfType,$resource=null,$context=null)
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;
        $viewSpecs = Config::getInstance()->getViewSpecifications($this->storeName);
        foreach($viewSpecs as $key=>$viewSpec)
        {
            // check for rdfType and rdfTypeAlias
            if (
                ($viewSpec["type"]==$rdfType || (is_array($viewSpec["type"]) && in_array($rdfType,$viewSpec["type"]))) ||
                ($viewSpec["type"]==$rdfTypeAlias || (is_array($viewSpec["type"]) && in_array($rdfTypeAlias,$viewSpec["type"]))) )
            {
                $foundSpec = true;
                $this->debugLog("Processing {$viewSpec['_id']}");
                $this->generateView($key,$resource,$context);
            }
        }
        if (!$foundSpec)
        {
            $this->debugLog("Could not find any view specifications for $resource with resource type '$rdfType'");
            return;
        }
    }

    /**
     * This method will delete all views where the _id.type of the viewmatches the specified $viewId
     * @param $viewId
     */
    public function deleteViewsByViewId($viewId){
        $viewSpec = Config::getInstance()->getViewSpecification($this->storeName, $viewId);
        if ($viewSpec==null)
        {
            $this->debugLog("Could not find a view specification with viewId '$viewId'");
            return;
        }

        $this->config->getCollectionForView($this->storeName, $viewId)
            ->remove(array("_id.type"=>$viewId), array('fsync'=>true));
    }

    /**
     * Given a specific $viewId, generates a single view for the $resource
     * @param string $viewId
     * @param string|null $resource
     * @param string|null $context
     * @param string|null $queueName Queue for background bulk generation
     * @throws \Tripod\Exceptions\ViewException
     * @return array
     */
    public function generateView($viewId,$resource=null,$context=null,$queueName=null)
    {
        $contextAlias = $this->getContextAlias($context);
        $viewSpec = Config::getInstance()->getViewSpecification($this->storeName, $viewId);
        if ($viewSpec==null)
        {
            $this->debugLog("Could not find a view specification for $resource with viewId '$viewId'");
            return null;
        }
        else
        {
            $t = new \Tripod\Timer();
            $t->start();

            $from = $this->getFromCollectionForViewSpec($viewSpec);
            $collection = $this->config->getCollectionForView($this->storeName, $viewId);

            if (!isset($viewSpec['joins']))
            {
                throw new \Tripod\Exceptions\ViewException('Could not find any joins in view specification - usecase better served with select()');
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
            if (isset($viewSpec['ensureIndexes']))
            {
                foreach ($viewSpec['ensureIndexes'] as $ensureIndex)
                {
                    $collection->ensureIndex(
                        $ensureIndex,
                        array(
                            'background'=>1
                        )
                    );
                }
            }

            $types = array(); // this is used to filter the CBD table to speed up the view creation
            if (is_array($viewSpec["type"]))
            {
                foreach ($viewSpec["type"] as $type)
                {
                    $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($type));
                    $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($type));
                }
            }
            else
            {
                $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($viewSpec["type"]));
                $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($viewSpec["type"]));
            }
            $filter = array('$or'=> $types);
            if (isset($resource))
            {
                $resourceAlias = $this->labeller->uri_to_alias($resource);
                $filter["_id"] = array(_ID_RESOURCE=>$resourceAlias,_ID_CONTEXT=>$contextAlias);
            }

            $docs = $this->config->getCollectionForCBD($this->storeName, $from)->find($filter);
            $docs->timeout(\Tripod\Mongo\Config::getInstance()->getMongoCursorTimeout());

            foreach ($docs as $doc)
            {
                if($queueName && !$resource)
                {
                    $subject = new ImpactedSubject(
                        $doc['_id'],
                        OP_VIEWS,
                        $this->storeName,
                        $from,
                        array($viewId)
                    );

                    $jobOptions = array();
                    if(isset($this->stat))
                    {
                        $jobOptions['statsConfig'] = $this->getStat()->getConfig();
                    }
                    elseif(!empty($this->statsConfig))
                    {
                        $jobOptions['statsConfig'] = $this->statsConfig;
                    }

                    $this->getApplyOperation()->createJob(array($subject), $queueName, $jobOptions);
                }
                else
                {
                    // set up ID
                    $generatedView = array("_id"=>array(_ID_RESOURCE=>$doc["_id"][_ID_RESOURCE],_ID_CONTEXT=>$doc["_id"][_ID_CONTEXT],_ID_TYPE=>$viewSpec['_id']));
                    $value = array(); // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R

                    $value[_GRAPHS] = array();

                    $buildImpactIndex=true;
                    if (isset($viewSpec['ttl']))
                    {
                        $buildImpactIndex=false;
                        $value[_EXPIRES] = new \MongoDate($this->getExpirySecFromNow($viewSpec['ttl']));
                    }
                    else
                    {
                        $value[_IMPACT_INDEX] = array($doc['_id']);
                    }

                    $this->doJoins($doc,$viewSpec['joins'],$value,$from,$contextAlias,$buildImpactIndex);

                    // add top level properties
                    $value[_GRAPHS][] = $this->extractProperties($doc,$viewSpec,$from);

                    $generatedView['value'] = $value;

                    $collection->save($generatedView);
                }
            }

            $t->stop();
            $this->timingLog(MONGO_CREATE_VIEW, array(
                'view'=>$viewSpec['type'],
                'duration'=>$t->result(),
                'filter'=>$filter,
                'from'=>$from));
            $this->getStat()->timer(MONGO_CREATE_VIEW.".$viewId",$t->result());
        }
    }

    /**
     * Joins data to $dest from $source according to specification in $joins, or queries DB if data is not available in $source.
     * @param $source
     * @param $joins
     * @param $dest
     * @param $from
     * @param $contextAlias
     * @param bool $buildImpactIndex
     */
    protected function doJoins($source, $joins, &$dest, $from, $contextAlias, $buildImpactIndex=true)
    {
        // expand sequences before doing any joins...
        $this->expandSequence($joins,$source);

        foreach ($joins as $predicate=>$ruleset) {
            if ($predicate=='followSequence') {
                continue;
            }

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
                else
                {
                    // multiple values for join
                    $joinsPushed = 0;
                    foreach ($source[$predicate] as $v)
                    {
                        if (isset($ruleset['maxJoins']) && !$joinsPushed<$ruleset['maxJoins'])
                        {
                            break; // maxJoins reached
                        }
                        $joinUris[] = array(_ID_RESOURCE=>$v[VALUE_URI],_ID_CONTEXT=>$contextAlias);
                        $joinsPushed++;
                    }
                }

                $recursiveJoins = array();
                $collection = (
                isset($ruleset['from'])
                    ? $this->config->getCollectionForCBD($this->storeName, $ruleset['from'])
                    : $this->config->getCollectionForCBD($this->storeName, $from)
                );
                $cursor = $collection->find(array('_id'=>array('$in'=>$joinUris)));
                $cursor->timeout(\Tripod\Mongo\Config::getInstance()->getMongoCursorTimeout());

                $this->addIdToImpactIndex($joinUris, $dest, $buildImpactIndex);
                foreach($cursor as $linkMatch) {
                    // if there is a condition, check it...
                    if (isset($ruleset['condition']))
                    {
                        $ruleset['condition']['._id'] = $linkMatch['_id'];
                    }
                    if (!(isset($ruleset['condition']) && $collection->count($ruleset['condition'])==0))
                    {
                        // make sure any sequences are expanded before extracting properties
                        if (isset($ruleset['joins'])) $this->expandSequence($ruleset['joins'],$linkMatch);

                        $dest[_GRAPHS][] = $this->extractProperties($linkMatch,$ruleset,$from);

                        if (isset($ruleset['joins']))
                        {
                            // recursive joins must be done after this cursor has completed, otherwise things get messy
                            $recursiveJoins[] = array('data'=>$linkMatch, 'ruleset'=>$ruleset['joins']);
                        }
                    }
                }
                if (count($recursiveJoins)>0)
                {
                    foreach ($recursiveJoins as $r)
                    {
                        $this->doJoins($r['data'],$r['ruleset'],$dest,$from,$contextAlias,$buildImpactIndex);
                    }
                }
            }
        }
        return;
    }


    /**
     * Returns a document with properties extracted from $source, according to $viewSpec. Useful for partial representations
     * of CBDs in a view
     * @param $source
     * @param $viewSpec
     * @param $from
     * @return array
     */
    protected function extractProperties($source,$viewSpec,$from)
    {
        $obj = array();
        if (isset($viewSpec['include']))
        {
            $obj['_id'] = $source['_id'];
            foreach ($viewSpec['include'] as $p)
            {
                if(isset($source[$p]))
                {
                    $obj[$p] = $source[$p];
                }
            }
            if (isset($viewSpec['joins']))
            {
                foreach ($viewSpec['joins'] as $p=>$join)
                {
                    if (isset($join['maxJoins']))
                    {
                        // todo: refactor with below (extract method)
                        // only include up to maxJoins
                        for ($i=0;$i<$join['maxJoins'];$i++)
                        {
                            if(isset($source[$p]) && (isset($source[$p][VALUE_URI]) || isset($source[$p][VALUE_LITERAL])) && $i==0) // cater for source with only one val
                            {
                                $obj[$p] = $source[$p];
                            }
                            if(isset($source[$p]) && isset($source[$p][$i]))
                            {
                                if (!isset($obj[$p])) $obj[$p] = array();
                                $obj[$p][] = $source[$p][$i];
                            }
                        }
                    }
                    else if(isset($source[$p]))
                    {
                        $obj[$p] = $source[$p];
                    }
                }
            }
        }
        else
        {
            foreach($source as $p=>$val)
            {
                if (isset($viewSpec['joins']) && isset($viewSpec['joins'][$p]) && isset($viewSpec['joins'][$p]['maxJoins']))
                {
                    // todo: refactor with above (extract method)
                    // only include up to maxJoins
                    for ($i=0;$i<$viewSpec['joins'][$p]['maxJoins'];$i++)
                    {
                        if($val && (isset($val[VALUE_URI]) || isset($val[VALUE_LITERAL])) && $i==0) // cater for source with only one val
                        {
                            $obj[$p] = $val;
                        }
                        if($val && isset($val[$i]))
                        {
                            if (!$obj[$p]) $obj[$p] = array();
                            $obj[$p][] = $val[$i];
                        }
                    }
                }
                else
                {
                    $obj[$p] = $val;
                }
            }
        }

        // process count aggregate function
        if (isset($viewSpec['counts']))
        {
            foreach ($viewSpec['counts'] as $predicate=>$c)
            {
                if (isset($c['filter'])) // run a db filter
                {
                    $collection = (isset($c['from'])
                        ? $this->config->getCollectionForCBD($this->storeName, $c['from'])
                        : $this->config->getCollectionForCBD($this->storeName, $from)
                    );
                    $query = $c['filter'];
                    $query[$c['property'].'.'.VALUE_URI] = $source['_id'][_ID_RESOURCE]; //todo: how does graph restriction work here?
                    $obj[$predicate] = array(VALUE_LITERAL=>$collection->count($query).''); // make sure it's a string
                }
                else // just look for property in current source...
                {
                    $count = 0;
                    // just count predicates at current location
                    if (isset($source[$c['property']]))
                    {
                        if (isset($source[$c['property']][VALUE_URI]) || isset($source[$c['property']][VALUE_LITERAL]))
                        {
                            $count = 1;
                        }
                        else
                        {
                            $count = count($source[$c['property']]);
                        }
                    }
                    $obj[$predicate] = array(VALUE_LITERAL=>(string)$count);
                }
            }
        }

        return $obj;
    }

    /**
     * @param string $viewSpec
     * @return string
     */
    private function getFromCollectionForViewSpec($viewSpec)
    {
        $from = null;
        if (isset($viewSpec["from"]))
        {
            $from = $viewSpec["from"];
        }
        else
        {
            $from = $this->podName;
        }
        return $from;
    }

    /**
     * @param string $viewSpecId
     * @return \MongoCollection
     */
    protected function getCollectionForViewSpec($viewSpecId)
    {
        return $this->getConfigInstance()->getCollectionForView($this->storeName, $viewSpecId);
    }

}