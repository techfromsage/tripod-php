<?php
require_once TRIPOD_DIR.'classes/ExtendedGraph.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodLabeller.class.php';
require_once TRIPOD_DIR . 'mongo/serializers/MongoTripodNQuadSerializer.class.php';

class MongoGraph extends ExtendedGraph {

    function __construct($configSpec = MongoTripodConfig::DEFAULT_CONFIG_SPEC)
    {
        $this->_labeller = new MongoTripodLabeller($configSpec);
    }

    /**
     * Given a context this method serializes the current graph to nquads of the form
     *  <s> <p> <o> <context> .
     * @param string $context the context for the graph your are serializing
     * @return string the nquad serialization of the graph
     * @throws InvalidArgumentException if you do not specify a context
     */
    function to_nquads($context)
    {
        if(empty($context)) {
            throw new InvalidArgumentException("You must specify the context when serializing to nquads");
        }

        $serializer = new MongoTripodNQuadSerializer();
        return $serializer->getSerializedIndex($this->_index, $this->_labeller->qname_to_alias($context));
    }

    /**
     * Adds the tripod array(s) to this graph.
     * This method is used to add individual tripod documents, or a series of tripod array documents that are embedded in a view.
     * @param $tarray
     * @throws TripodException
     */
    function add_tripod_array($tarray)
    {
        if (!is_array($tarray))
        {
            throw new TripodException("Value passed to add_tripod_array is not of type array");
        }
        // need to convert from tripod storage format to rdf/json as php array format
        if (isset($tarray["value"][_GRAPHS]))
        {
            // iterate add add each graph
            foreach($tarray["value"][_GRAPHS] as $graph)
            {
                $this->add_tarray_to_index($graph);
            }
        }
        else
        {
            $this->add_tarray_to_index($tarray);
        }
    }

    /**
     * Returns a mongo-ready doc for a single CBD
     * @param $docId
     * @param $context
     * @return array
     */
    function to_tripod_array($docId,$context)
    {
        $docId = $this->_labeller->qname_to_alias($docId);
        $contextAlias = $this->_labeller->uri_to_alias($context);

        if ($docId!=null)
        {
            $subjects = $this->get_subjects();
            foreach ($subjects as $subject)
            {
                if ($subject==$docId)
                {
                    $graph = $this->get_subject_subgraph($subject);
                    $doc = $this->index_to_tarray($graph,$contextAlias);
                    return $doc;
                }
            }
        }
        return null;
    }

    /**
     * Returns a mongo-ready doc for views, which can have multiple graphs in the same doc
     * @param $docId
     * @param $context
     * @return array
     */
    function to_tripod_view_array($docId,$context)
    {
        $subjects = $this->get_subjects();
        $contextAlias = $this->_labeller->uri_to_alias($context);

        // view
        $doc = array();
        $doc["_id"] = array(_ID_RESOURCE=>$docId,_ID_CONTEXT=>$contextAlias);
        $doc["value"] = array();
        $doc["value"][_IMPACT_INDEX] = $subjects;
        $doc["value"][_GRAPHS] = array();
        foreach ($subjects as $subject)
        {
            $graph = $this->get_subject_subgraph($subject);
            $doc["value"][_GRAPHS][] = $this->index_to_tarray($graph,$contextAlias);
        }
        return $doc;
    }
    
    private function add_tarray_to_index($tarray)
    {
        $_i = array();
        $predObjects = array();
        foreach ($tarray as $key=>$value)
        {
            if($key[0] != '_')
            {
                $predicate = $this->qname_to_uri($key);
                $predObjects[$predicate] = $this->toGraphValueObject($value);
            }
        }
        $_i[$this->_labeller->qname_to_alias($tarray["_id"][_ID_RESOURCE])] = $predObjects;
        $this->add_json(json_encode($_i));
    }

    /**
     * Convert from MongoTripod value object format (comapct) to ExtendedGraph format (verbose)
     * @param $mongoValueObject
     * @return array
     */
    private function toGraphValueObject($mongoValueObject)
    {
        $simpleGraphValueObject = null;

        if (array_key_exists(VALUE_LITERAL,$mongoValueObject))
        {
            // single value literal
            $simpleGraphValueObject[] = array(
                'type'=>'literal',
                'value'=>$mongoValueObject[VALUE_LITERAL]);
        }
        else if (array_key_exists(VALUE_URI,$mongoValueObject))
        {
            // single value literal
            $simpleGraphValueObject[] = array(
                'type'=>'uri',
                'value'=>$this->_labeller->qname_to_alias($mongoValueObject[VALUE_URI]));
        }
        else
        {
            foreach ($mongoValueObject as $kvp)
            {
                foreach ($kvp as $type=>$value)
                {
                    $simpleGraphValueObject[] = array(
                        'type'=>($type==VALUE_LITERAL) ? 'literal' : 'uri',
                        'value'=>($type==VALUE_URI) ? $this->_labeller->qname_to_alias($value) : $value);
                }
            }
        }
        return $simpleGraphValueObject;
    }

    /**
     * Convert from ExtendedGraph value object format (verbose) to MongoTripod format (compact)
     * @param $simpleGraphValueObject
     * @return array
     */
    private function toMongoTripodValueObject($simpleGraphValueObject)
    {
        $valueTypeProp = ($simpleGraphValueObject['type']=="literal") ? VALUE_LITERAL : VALUE_URI;
        return array(
            $valueTypeProp=>
            ($simpleGraphValueObject['type']=="literal") ? $simpleGraphValueObject['value'] : $this->_labeller->uri_to_alias($simpleGraphValueObject['value']));
    }

    private function index_to_tarray($graph=null,$contextAlias)
    {
        if ($graph==null) $graph = $this;
        $_i = $graph->_index;

        foreach ($_i as $resource=>$predObjects)
        {
            $doc = array();
            $id = array();

            $id[_ID_RESOURCE] = $this->_labeller->uri_to_alias($resource);
            $id[_ID_CONTEXT] = $contextAlias;
            $doc["_id"] = $id;
            foreach ($predObjects as $predicate=>$objects)
            {
                $pQName = $this->uri_to_qname($predicate);
                if (count($objects)==1)
                {
                    $doc[$pQName] = $this->toMongoTripodValueObject($objects[0]);
                }
                else
                {
                    $values = array();
                    foreach ($objects as $obj)
                    {
                        $values[] = $this->toMongoTripodValueObject($obj);
                    }
                    $doc[$pQName] = $values;
                }
            }
            return $doc; // we assume $_i is a single subject graph
        }

        return null;
    }
}