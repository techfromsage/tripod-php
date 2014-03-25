<?php

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR . 'mongo/base/MongoTripodBase.class.php';

class MongoTripodTables extends MongoTripodBase implements SplObserver
{
    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * MongoTripod and should inherit connections set up there
     * @param MongoDB $db
     * @param MongoCollection $collection
     * @param $defaultContext
     * @param $stat
     */
    function __construct(MongoDB $db,MongoCollection $collection,$defaultContext,$stat=null)
    {
        $this->labeller = new MongoTripodLabeller();
        $this->db = $db;
        $this->collection = $collection;
        $this->collectionName = $collection->getName();
        $this->defaultContext = $this->labeller->uri_to_alias($defaultContext); // make sure default context is qnamed if applicable
        $this->stat = $stat;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Receive update from subject
     * @link http://php.net/manual/en/splobserver.update.php
     * @param SplSubject $subject <p>
     * The <b>SplSubject</b> notifying the observer of an update.
     * </p>
     * @return void
     */
    public function update(SplSubject $subject)
    {
        /* @var $subject ModifiedSubject */
        $queuedItem = $subject->getData();
        $resourceUri    = $queuedItem[_ID_RESOURCE];
        $context        = $queuedItem[_ID_CONTEXT];

        if(isset($queuedItem['delete']))
        {
            $this->deleteTableRowsForResource($resourceUri,$context);
        }
        else
        {
            $this->generateTableRowsForResource($resourceUri,$context);
        }
    }

    public function getTableRows($tableSpecId,$filter=array(),$sortBy=array(),$offset=0,$limit=10)
    {
        $t = new Timer();
        $t->start();

        $filter["_id.type"] = $tableSpecId;

        $collection = $this->db->selectCollection(TABLE_ROWS_COLLECTION);
        $results = (empty($limit)) ? $collection->find($filter) : $collection->find($filter)->skip($offset)->limit($limit);
        if (isset($sortBy))
        {
            $results->sort($sortBy);
        }
        $rows = array();
        foreach ($results as $doc)
        {
            if (array_key_exists(_IMPACT_INDEX,$doc['value'])) unset($doc['value'][_IMPACT_INDEX]); // remove impact index from client
            $rows[] = $doc['value'];
        }

        $t->stop();
        $this->timingLog(MONGO_TABLE_ROWS, array('duration'=>$t->result(), 'query'=>$filter, 'collection'=>TABLE_ROWS_COLLECTION));
        $this->getStat()->timer(MONGO_TABLE_ROWS.".$tableSpecId",$t->result());

        return array(
            "head"=>array(
                "count"=>$results->count(),
                "offset"=>$offset,
                "limit"=>$limit
            ),
            "results"=>$rows);
    }

    protected function deleteTableRowsForResource($resource, $context=null)
    {
        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $this->db->selectCollection(TABLE_ROWS_COLLECTION)->remove(array("_id.r"=>$resourceAlias,"_id.c"=>$contextAlias));
    }

    /**
     * This method will delete all table rows where the _id.type matches the specified $tableId
     * @param $tableId
     */
    public function deleteTableRowsByTableId($tableId) {
        $tableSpec = MongoTripodConfig::getInstance()->getTableSpecification($tableId);
        if ($tableSpec==null)
        {
            $this->debugLog("Cound not find a table specification for $tableId");
            return;
        }

        $this->db->selectCollection(TABLE_ROWS_COLLECTION)->remove(array("_id.type"=>$tableId), array('fsync'=>true));
    }

    /**
     * This method handles invalidation and regeneration of table rows based on impact index, before deligating to
     * generateTableRowsForType() for re-generation of any table rows for the $resource
     * @param $resource
     * @param null $context
     */
    protected function generateTableRowsForResource($resource, $context=null)
    {
        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        // delete any rows with this resource and context in the key
        foreach (MongoTripodConfig::getInstance()->getTableSpecifications() as $type=>$spec)
        {
            if ($spec['from']==$this->collectionName){
                $this->db->selectCollection(TABLE_ROWS_COLLECTION)->remove(array("_id" => array("r"=>$resourceAlias,"c"=>$contextAlias,"type"=>$type)));
            }
        }

        $filter = array();
        $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);

        // now go through the types
        $query = array("_id"=>array('$in'=>$filter));
        $resourceAndType = $this->db->selectCollection($this->collectionName)->find($query,array("_id"=>1,"rdf:type"=>1));

        foreach ($resourceAndType as $rt)
        {
            $id = $rt["_id"];
            if (array_key_exists("rdf:type",$rt))
            {
                if (array_key_exists('u',$rt["rdf:type"]))
                {
                    // single type, not an array of values
                    $this->generateTableRowsForType($rt["rdf:type"]['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                }
                else
                {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type)
                    {
                        $this->generateTableRowsForType($type['u'],$id[_ID_RESOURCE],$id[_ID_CONTEXT]);
                    }
                }
            }
        }
    }

    /**
     * Given a set of resources, this method returns the ids of the documents that are directly affected.
     * As a note remember that if ResourceA has a view associated with it, then the impactIndex for ResourceA, will contain
     * an entry for ResourceA as well as any other Resources.
     * @param $resources
     * @param null $context
     * @return array
     */
    public function findImpactedTableRows($resources, $context = null)
    {
        $contextAlias = $this->getContextAlias($context);

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
        $tableRows = $this->db->selectCollection(TABLE_ROWS_COLLECTION)->find($query,array("_id"=>true));

        $affectedTableRows = array();

        foreach($tableRows as $t)
        {
            $affectedTableRows[] = $t;
        }

        return $affectedTableRows;
    }


    /**
     * This method finds all the table specs for the given $rdfType and generates the table rows for the $subject one by one
     * @param $rdfType
     * @param null $subject
     * @param null $context
     * @return mixed
     * @throws Exception
     */
    public function generateTableRowsForType($rdfType,$subject=null,$context=null)
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;
        $tableSpecs = MongoTripodConfig::getInstance()->getTableSpecifications();
        foreach($tableSpecs as $key=>$tableSpec)
        {
            if ($tableSpec["type"]==$rdfType || $tableSpec["type"]==$rdfTypeAlias)
            {
                $foundSpec = true;
                $this->debugLog("Processing {$tableSpec['_id']}");
                $this->generateTableRows($key,$subject,$context);
            }
        }
        if (!$foundSpec)
        {
            $this->debugLog("Cound not find any table specifications for $subject with resource type '$rdfType'");
            return;
        }
    }

    public function generateTableRows($tableType,$resource=null,$context=null)
    {
        $t = new Timer();
        $t->start();

        $tableSpec = MongoTripodConfig::getInstance()->getTableSpecification($tableType);

        if ($tableSpec==null)
        {
            $this->debugLog("Cound not find a table specification for $tableType");
            return null;
        }

        // ensure both the ID field and the impactIndex indexes are correctly set up
        $this->db->selectCollection(TABLE_ROWS_COLLECTION)->ensureIndex(array('_id.r'=>1, '_id.c'=>1,'_id.type'=>1),array('background'=>1));
        $this->db->selectCollection(TABLE_ROWS_COLLECTION)->ensureIndex(array('value.'._IMPACT_INDEX=>1),array('background'=>1));

        // ensure any custom view indexes
        if (isset($tableSpec['ensureIndexes']))
        {
            foreach ($tableSpec['ensureIndexes'] as $ensureIndex)
            {
                $this->db->selectCollection(TABLE_ROWS_COLLECTION)->ensureIndex($ensureIndex,array('background'=>1));
            }
        }

        // default the context
        $contextAlias = $this->getContextAlias($context);

        // default collection
        $from = (isset($tableSpec["from"])) ? $tableSpec["from"] : $this->collectionName;

        $filter = array(); // this is used to filter the CBD table to speed up the view creation
        $types = array();
        if (is_array($tableSpec["type"]))
        {
            foreach ($tableSpec["type"] as $type)
            {
                $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($type));
                $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($type));
            }
        }
        else
        {
            $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($tableSpec["type"]));
            $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($tableSpec["type"]));
        }
        $filter = array('$or'=> $types);
        if (isset($resource))
        {
            $filter["_id"] = array(_ID_RESOURCE=>$this->labeller->uri_to_alias($resource),_ID_CONTEXT=>$contextAlias);
//            $result =  $this->doMapReduce($from, $map, $reduce, $filter);
        }
//        else
//        {
//            $i=0;
//            $this->doBulkMR($from, $tableSpec, $filter, $map, $reduce, $i); // todo: We are not detecting failure of individual m-r's here... fix
//        }
        $docs = $this->db->selectCollection($from)->find($filter);
        foreach ($docs as $doc)
        {
            // set up ID
            $generatedRow = array("_id"=>array(_ID_RESOURCE=>$doc["_id"][_ID_RESOURCE],_ID_CONTEXT=>$doc["_id"][_ID_CONTEXT],_ID_TYPE=>$tableSpec['_id']));

            $value = array('_id'=>$doc['_id']); // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R
            $value[_IMPACT_INDEX] = array($doc['_id']); // need to add the doc to the impact index to be consistent with views/search etc. this is needed for discovering impacted operations
            $this->addFields($doc,$tableSpec,$value);
            if (isset($tableSpec['joins']))
            {
                $this->doJoins($doc,$tableSpec['joins'],$value,$from,$contextAlias);
            }

            $generatedRow['value'] = $value;
            $this->db->selectCollection(TABLE_ROWS_COLLECTION)->save($generatedRow);
        }


            $t->stop();
        $this->timingLog(MONGO_CREATE_TABLE, array(
            'type'=>$tableSpec['type'],
            'duration'=>$t->result(),
            'filter'=>$filter,
            'from'=>$from));
        $this->getStat()->timer(MONGO_CREATE_TABLE.".$tableType",$t->result());
//        return $result;
    }

    // a helper function that returns an object with properties included from the source,
    // as per the view spec
//$fn_addFields = new MongoCode("function (source,indexSpec,dest) {
//        if (indexSpec.fields)
//        {
//            for each (f in indexSpec.fields)
//            {
//                for each (p in f.predicates)
//                {
//                    if (source[p])
//                    {
//                        values = new Array();
//                        if (source[p]['".VALUE_URI."'])
//                        {
//                            values.push(source[p]['".VALUE_URI."']);
//                        }
//                        else if (source[p]['".VALUE_LITERAL."'])
//                        {
//                            values.push(source[p]['".VALUE_LITERAL."']);
//                        }
//                        else if (source[p]['"._ID_RESOURCE."']) // field being joined is the _id, will have _id{r:'',c:''}
//                        {
//                            values.push(source[p]['"._ID_RESOURCE."']);
//                        }
//                        else
//                        {
//                            for each (v in source[p])
//                            {
//                                if (v['".VALUE_LITERAL."'])
//                                {
//                                    values.push(v['".VALUE_LITERAL."']);
//                                }
//                                else if (v['".VALUE_URI."'])
//                                {
//                                    values.push(v['".VALUE_URI."']);
//                                }
//                                // _id's shouldn't appear in value arrays, so no need for third condition here
//                            }
//                        }
//                        // now add all the values
//                        for each (v in values)
//                        {
//                            if (!dest[f.fieldName])
//                            {
//                                // single value
//                                dest[f.fieldName] = v;
//                            }
//                            else if (dest[f.fieldName] instanceof Array)
//                            {
//                                // add to existing array of values
//                                dest[f.fieldName].push(v);
//                            }
//                            else
//                            {
//                                // convert from single value to array of values
//                                existingVal = dest[f.fieldName];
//                                dest[f.fieldName] = new Array();
//                                dest[f.fieldName].push(existingVal);
//                                dest[f.fieldName].push(v);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        }");

    protected function addFields($source,$spec,&$dest)
    {
        if (isset($spec['fields']))
        {
            foreach ($spec['fields'] as $f)
            {
                foreach ($f['predicates'] as $p)
                {
                    if (isset($source[$p]))
                    {
                        $values = array();
                        if (isset($source[$p][VALUE_URI]) && !empty($source[$p][VALUE_URI]))
                        {
                            $values[] = $source[$p][VALUE_URI];
                        }
                        else if (isset($source[$p][VALUE_LITERAL]) && !empty($source[$p][VALUE_LITERAL]))
                        {
                            $values[] = $source[$p][VALUE_LITERAL];
                        }
                        else if (isset($source[$p][_ID_RESOURCE])) // field being joined is the _id, will have _id{r:'',c:''}
                        {
                            $values[] = $source[$p][_ID_RESOURCE];
                        }
                        else
                        {
                            foreach ($source[$p] as $v)
                            {
                                if (isset($v[VALUE_LITERAL]) && !empty($v[VALUE_LITERAL]))
                                {
                                    $values[] = $v[VALUE_LITERAL];
                                }
                                else if (isset($v[VALUE_URI]) && !empty($v[VALUE_URI]))
                                {
                                    $values[] = $v[VALUE_URI];
                                }
                                // _id's shouldn't appear in value arrays, so no need for third condition here
                            }
                        }
                        // now add all the values
                        foreach ($values as $v)
                        {
                            if (!isset($dest[$f['fieldName']]))
                            {
                                // single value
                                $dest[$f['fieldName']] = $v;
                            }
                            else if (is_array($dest[$f['fieldName']]))
                            {
                                // add to existing array of values
                                $dest[$f['fieldName']][] = $v;
                            }
                            else
                            {
                                // convert from single value to array of values
                                $existingVal = $dest[$f['fieldName']];
                                $dest[$f['fieldName']] = array();
                                $dest[$f['fieldName']][] = $existingVal;
                                $dest[$f['fieldName']][] = $v;
                            }
                        }
                    }
                }

                // Allow the value to be modified after generation
                if(isset($f['modifiers']))
                {
                    foreach($f['modifiers'] as $m)
                    {
                        if(isset($dest[$f['fieldName']]))
                        {
                            $dest[$f['fieldName']] = $this->applyModifiers($m, $dest[$f['fieldName']]);
                        }
                    }
                }

                // Allow URI linking to the ID
                if(isset($f['value']))
                {
                    if(strpos($f['value'], ':') !== false)
                    {
                        list($fieldValue, $type) = explode(':', $f['value']);
                        if($fieldValue == '_link_')
                        {
                            switch($type)
                            {
                                case '_user_':
                                    $value = $this->labeller->qname_to_alias($dest['_id']['r']);
                                    break;
                            }
                            $dest[$f['fieldName']] = $value;
                        }
                    } else
                    {
                        $dest[$f['fieldName']] = '';
                    }
                }

            }
        }
    }

    /**
     * Loop through modifiers and apply each one in turn to the value
     * @param array $modifiers
     * @param mixed $value
     * @access protected
     * @return string
     */
    protected function applyModifiers(array $modifiers, $value)
    {
        // Modifiers has to be an array
        if(is_array($modifiers))
        {
            foreach($modifiers as $modifier => $options)
            {
                // Apply the modifier
                $value = $this->applyModifier($modifier, $value, $options);
            }
        }
        return $value;
    }

    /**
     * Apply a specific modifier
     * Options you can use are
     *      lowerCase - no options
     *      join - pass in "glue":" " to specify what to glue multiple values together with
     *      mongoDate - no options
     * @param string $modifier
     * @param string $value
     * @param array $options
     * @return mixed
     */
    private function applyModifier($modifier, $value, $options = array())
    {
        switch($modifier)
        {
            case 'lowerCase':
                if(is_string($value)) $value = strtolower($value);
                break;
            case 'join':
                if(is_array($value)) $value = implode($options['glue'], $value);
                break;
            case 'mongoDate':
                $value = new MongoDate(strtotime($value));
                break;
        }

        return $value;
    }

//        // this is the javascript function that will do most of the work. Based on the viewspec, it will
//        // query the DB and follow joins (recursively) to build the view.
//        $fn_doJoin = new MongoCode("function (source,joins,dest) {
////         uncomment the following two lines if you need debug in the output...
////                 if (!dest.log)
////                     dest.log = new Array();
//        if (joins.followSequence)
//        {
//            // add any rdf:_x style properties in the source to the joins array,
//            // up to rdf:_1000 (unless a max is specified in the spec)
//            var max = (joins.followSequence.maxJoins) ? joins.followSequence.maxJoins : 1000;
//            for (var i=0;i<max;i++)
//            {
//                if (source['rdf:_'+(i+1)])
//                {
//                    joins['rdf:_'+(i+1)] = joins.followSequence;
//                }
//                else
//                {
//                    // no more sequence properties
//                    break;
//                }
//            }
//        }
//
//        for (predicate in joins) {
//            ruleset = joins[predicate];
//            if (ruleset['include'])
//            {
//                properties = ruleset['include'];
//            }
//            else
//            {
//                properties = null;
//            }
//
//            if (source[predicate])
//            {
//                // todo: perhaps we can get better performance by detecting whether or not
//                // the uri to join on is already in the impact index, and if so not attempting
//                // to join on it. However, we need to think about different combinations of
//                // nested joins in different points of the view spec and see if this would
//                // complicate things. Needs a unit test or two.
//                joinUris = new Array();
//                if (source[predicate]['".VALUE_URI."'])
//                {
//                    // single value for join
//                    joinUris.push({"._ID_RESOURCE.":source[predicate]['".VALUE_URI."'],"._ID_CONTEXT.":'$contextAlias'});
//                }
//                else if(predicate == '_id')
//                {
//                    // single value for join
//                    joinUris.push({"._ID_RESOURCE.":source[predicate]['"._ID_RESOURCE."'],"._ID_CONTEXT.":'$contextAlias'});
//                }
//                else
//                {
//                    // multiple values for join
//                    for each (var v in source[predicate])
//                    {
//                        joinUris.push({"._ID_RESOURCE.":v['".VALUE_URI."'],"._ID_CONTEXT.":'$contextAlias'});
//                    }
//                }
//                recursiveJoins = new Array();
//                var collection = (ruleset.from) ? db.getCollection(ruleset.from) : db.getCollection('$from');
//                for( var c = collection.find({'_id':{\$in:joinUris}}); c.hasNext(); ) {
//                    linkMatch = c.next();
//
//                    if (!dest['"._IMPACT_INDEX."'])
//                      dest['"._IMPACT_INDEX."'] = new Array();
//
//                    // add linkMatch if there isn't already a graph for it in the dest obj
//                    if (dest['"._IMPACT_INDEX."'].indexOf(linkMatch['_id'])==-1)
//                    {
//                        dest['"._IMPACT_INDEX."'].push(linkMatch['_id']);
//                    }
//
//                    addFields(linkMatch,ruleset,dest);
//
//                    if (ruleset.joins)
//                    {
//                        // recursive joins must be done after this cursor has completed, otherwise things get messy
//                        recursiveJoins.push({data: linkMatch, ruleset: ruleset.joins});
//                    }
//                }
//                if (recursiveJoins.length>0)
//                {
//                    for each (var r in recursiveJoins)
//                    {
//                        fn_doJoin(r.data,r.ruleset,dest);
//                    }
//                }
//            }
//        }
//        return;
//        }");

    protected function doJoins($source,$joins,&$dest,$from,$contextAlias)
    {
//        // this is the javascript function that will do most of the work. Based on the viewspec, it will
//        // query the DB and follow joins (recursively) to build the view.
//        $fn_doJoin = new MongoCode("function (source,joins,dest) {
////         uncomment the following two lines if you need debug in the output...
////                 if (!dest.log)
////                     dest.log = new Array();
        $this->expandSequence($joins,$source);
        foreach ($joins as $predicate=>$ruleset)
        {
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
                else if($predicate == '_id')
                {
                    // single value for join
                    $joinUris[] = array(_ID_RESOURCE=>$source[$predicate][_ID_RESOURCE],_ID_CONTEXT=>$contextAlias);
                }
                else
                {
                    // multiple values for join
                    foreach ($source[$predicate] as $v)
                    {
                        $joinUris[] = array(_ID_RESOURCE=>$v[VALUE_URI],_ID_CONTEXT=>$contextAlias);
                    }
                }

                $recursiveJoins = array();
                $collection = (isset($ruleset['from'])) ? $this->db->selectCollection($ruleset['from']) : $this->db->selectCollection($from);
                $cursor = $collection->find(array('_id'=>array('$in'=>$joinUris)));
                foreach($cursor as $linkMatch) {
                    if (!isset($dest[_IMPACT_INDEX])) $dest[_IMPACT_INDEX] = array();

                    // add linkMatch if there isn't already a graph for it in the dest obj
                    $dest[_IMPACT_INDEX][] = $linkMatch['_id']; // todo: only add if doesn't already exist

                    $this->addFields($linkMatch,$ruleset,$dest);

                    if (isset($ruleset['joins']))
                    {
                        // recursive joins must be done after this cursor has completed, otherwise things get messy
                        $recursiveJoins[] = array('data'=>$linkMatch, 'ruleset'=>$ruleset['joins']);
                    }
                }
                if (count($recursiveJoins)>0)
                {
                    foreach ($recursiveJoins as $r)
                    {
                        $this->doJoins($r['data'],$r['ruleset'],$dest,$from,$contextAlias);
                    }
                }
            }
        }
    }
}
