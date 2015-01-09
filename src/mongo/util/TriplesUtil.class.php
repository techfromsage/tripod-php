<?php
/**
 * Created by Chris Clarke
 * Date: 12/01/2012
 * Time: 16:04
 * Class to help working with triples and MongoTripod
 */

require_once(TRIPOD_DIR.'mongo/MongoGraph.class.php');

class TriplesUtil
{
    private $collections = array();
    private $labeller = null;

    function __construct()
    {
        $this->labeller = new MongoTripodLabeller();
    }


    private function isUri($object)
    {
        return filter_var($object, FILTER_VALIDATE_URL);
    }

    private function extract_object($parts)
    {
        if( !$this->is_object_literal($parts[2]) )
        {
            return trim($parts[2], "><");
        }

        $sliced = array_slice($parts, 2);

        $str = implode(" ", $sliced);
        $str = preg_replace('@"[^"]*$@','',$str); // get rid of xsd typing

        $str = substr($str,1,strlen($str)-1);//trim($str, "\"");

        $json_string = "{\"string\":\"". (str_replace("\u","\\u", $str)) ."\"}";
        $json = json_decode($json_string, true);
        if(!empty($json)) {
            $str = $json["string"];
        }
        return $str;
    }

    private function is_object_literal($input)
    {
        if($input[0]=='"')
        {
            return true;
        }
        return false;
    }

    /**
     * Add $triples about a given $subject to Mongo. Only $triples with subject matching $subject will be added, others will be ignored.
     * Make them quads with a $context
     * @param $subject
     * @param array $triples
     * @param string $storeName
     * @param string $podName
     * @param null $context
     * @param null $allowableTypes
     */
    public function loadTriplesAbout($subject,Array $triples,$storeName,$podName,$context=null,$allowableTypes=null)
    {
        $context = ($context==null) ? MongoTripodConfig::getInstance()->getDefaultContextAlias() : $this->labeller->uri_to_alias($context);
        if (array_key_exists($podName,$this->collections))
        {
            $collection = $this->collections[$podName];
        }
        else
        {
            $m = new MongoClient(MongoTripodConfig::getInstance()->getConnStr($storeName));
            $collection = $m->selectDB($storeName)->selectCollection($podName);
        }

        $graph = new MongoGraph();
        foreach ($triples as $triple)
        {
            $triple = rtrim($triple);

            $parts = preg_split("/\s/",$triple);
            $subject = trim($parts[0],'><');
            $predicate = trim($parts[1],'><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object))
            {
                $graph->add_resource_triple($subject,$predicate,$object);
            }
            else
            {
                $graph->add_literal_triple($subject,$predicate,$object);
            }
        }
        if ($allowableTypes!=null && is_array($allowableTypes))
        {
            $types = $graph->get_resource_triple_values($subject,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
            if ($types==null || empty($types))
            {
                return;
            }
            else
            {
                foreach ($types as $type)
                {
                    if (in_array($type,$allowableTypes))
                    {
                        $this->saveCBD($subject,$graph,$collection,$context);
                        return;
                    }
                }
                return;
            }
        }
        $this->saveCBD($subject,$graph,$collection,$context);
    }

    /**
     * Add $triples about a given $subject to Mongo. Only $triples with subject matching $subject will be added, others will be ignored.
     * Make them quads with a $context
     * @param $subject
     * @param array $triples
     * @param null $context
     * @return array
     */
    public function bsonizeTriplesAbout($subject,Array $triples,$context=null)
    {
        $context = ($context==null) ? MongoTripodConfig::getInstance()->getDefaultContextAlias() : $this->labeller->uri_to_alias($context);
        $graph = new MongoGraph();
        foreach ($triples as $triple)
        {
            $triple = rtrim($triple);

            $parts = preg_split("/\s/",$triple);
            $subject = trim($parts[0],'><');
            $predicate = trim($parts[1],'><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object))
            {
                $graph->add_resource_triple($subject,$predicate,$object);
            }
            else
            {
                $graph->add_literal_triple($subject,$predicate,$object);
            }
        }
        return $graph->to_tripod_array($subject,$context);
    }

    public function extractMissingPredicateNs($triples)
    {
        $missingNs = array();
        $graph = new MongoGraph();
        foreach ($triples as $triple)
        {
            $triple = rtrim($triple);

            $parts = preg_split("/\s/",$triple);
            $predicate = trim($parts[1],'><');

            try
            {
                $graph->uri_to_qname($predicate);
            }
            catch (TripodLabellerException $te)
            {
                $missingNs[] = $te->getTarget();
            }
        }
        return array_unique($missingNs);
    }

    public function extractMissingObjectNs($triples)
    {
        $missingNs = array();
        $graph = new MongoGraph();
        foreach ($triples as $triple)
        {
            $triple = rtrim($triple);

            $parts = preg_split("/\s/",$triple);
            $object = $this->extract_object($parts);

            if ($this->isUri($object))
            {
                try
                {
                    $graph->uri_to_qname($object);
                }
                catch (TripodLabellerException $te)
                {
                    $missingNs[] = $te->getTarget();
                }
            }
        }
        return array_unique($missingNs);
    }

    public function suggestPrefix($ns)
    {
        $parts = preg_split('/[\/#]/', $ns);
        for ($i = count($parts) - 1; $i >= 0; $i--) {
            if (preg_match('~^[a-zA-Z][a-zA-Z0-9\-]+$~', $parts[$i]) && $parts[$i] != 'schema' && $parts[$i] != 'ontology' && $parts[$i] != 'vocab' && $parts[$i] != 'terms' && $parts[$i] != 'ns' && $parts[$i] != 'core' && strlen($parts[$i]) > 3) {
                $prefix = strtolower($parts[$i]);
                return $prefix;
            }
        }
        return 'unknown'.uniqid();
    }

    public function getTArrayAbout($subject,Array $triples,$context)
    {
        $graph = new MongoGraph();
        foreach ($triples as $triple)
        {
            $triple = rtrim($triple);

            $parts = preg_split("/\s/",$triple);
            $subject = trim($parts[0],'><');
            $predicate = trim($parts[1],'><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object))
            {
                $graph->add_resource_triple($subject,$predicate,$object);
            }
            else
            {
                $graph->add_literal_triple($subject,$predicate,$object);
            }
        }
        return $graph->to_tripod_array($subject,$context);
    }

    protected function saveCBD($cbdSubject,MongoGraph $cbdGraph,MongoCollection $collection,$context)
    {
        $cbdSubject = $this->labeller->uri_to_alias($cbdSubject);
        if ($cbdGraph == null || $cbdGraph->is_empty())
        {
            throw new Exception("graph for $cbdSubject was null");
        }
        try
        {
            $collection->insert($cbdGraph->to_tripod_array($cbdSubject,$context),array("w"=>1));
            print ".";
        }
        catch (MongoException $e)
        {
            if (preg_match('/E11000/',$e->getMessage()))
            {
                print "M";
                // key already exists, merge it
                $criteria = array("_id"=>array("r"=>$cbdSubject,"c"=>$context));
                $existingGraph = new MongoGraph();
                $existingGraph->add_tripod_array($collection->findOne($criteria));
                $existingGraph->add_graph($cbdGraph);
                try
                {
                    $collection->update($criteria,$existingGraph->to_tripod_array($cbdSubject,$context),array("w"=>1));
                }
                catch (MongoException $e2)
                {
                    throw new Exception($e2->getMessage()); // todo: would be good to have typed exception
                }
            }
            else
            {
                // retry
                print "MongoCursorException on update: ".$e->getMessage().", retrying\n";
                try
                {
                    $collection->insert($cbdGraph->to_tripod_array($cbdSubject,$context),array("w"=>1));
                }
                catch (MongoException $e2)
                {
                    throw new Exception($e2->getMessage()); // todo: would be good to have typed exception
                }
            }
        }
    }
}

