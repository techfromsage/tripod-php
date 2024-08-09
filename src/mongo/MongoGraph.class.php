<?php

namespace Tripod\Mongo;

require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';

/**
 * Class MongoGraph
 * @package Tripod\Mongo
 */
class MongoGraph extends \Tripod\ExtendedGraph {
    /**
     * @var Labeller
     */
    var $_labeller;

    /**
     * Constructor
     */
    function __construct()
    {
        $this->_labeller = new Labeller();
    }

    /**
     * Given a context this method serializes the current graph to nquads of the form
     *  <s> <p> <o> <context> .
     * @param string $context the context for the graph your are serializing
     * @return string the nquad serialization of the graph
     * @throws \InvalidArgumentException if you do not specify a context
     */
    function to_nquads($context)
    {
        if(empty($context)) {
            throw new \InvalidArgumentException("You must specify the context when serializing to nquads");
        }

        $serializer = new NQuadSerializer();
        return $serializer->getSerializedIndex($this->_index, $this->_labeller->qname_to_alias($context));
    }

    /**
     * Adds the tripod array(s) to this graph.
     * This method is used to add individual tripod documents, or a series of tripod array documents that are embedded in a view.
     * @param $tarray
     * @throws \Tripod\Exceptions\Exception
     */
    function add_tripod_array($tarray)
    {
        if (!is_array($tarray))
        {
            throw new \Tripod\Exceptions\Exception("Value passed to add_tripod_array is not of type array");
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

    /**
     * @param array $tarray
     * @throws \Tripod\Exceptions\Exception
     */
    private function add_tarray_to_index($tarray)
    {
        $_i = array();
        $predObjects = array();
        foreach ($tarray as $key=>$value)
        {
            if (empty($key))
            {
                throw new \Tripod\Exceptions\Exception("The predicate cannot be an empty string");
            }
            else if($key[0] != '_')
            {
                $predicate = $this->qname_to_uri($key);
                $graphValueObject = $this->toGraphValueObject($value);
                // Only add if valid values have been found
                if (!empty($graphValueObject))
                {
                    $predObjects[$predicate] = $graphValueObject;
                }
            }
            else if($key == "_id")
            {
                // If the subject is invalid then throw an exception
                if(!isset($value['r']) || !$this->isValidResource($value['r']))
                {
                    throw new \Tripod\Exceptions\Exception("The subject cannot be an empty string");
                }
            }
        }
        $_i[$this->_labeller->qname_to_alias($tarray["_id"][_ID_RESOURCE])] = $predObjects;
        $this->add_json(json_encode($_i));
    }

    /**
     * Convert from Tripod value object format (comapct) to ExtendedGraph format (verbose)
     *
     * @param array $mongoValueObject
     * @return array
     */
    private function toGraphValueObject($mongoValueObject)
    {
        $simpleGraphValueObject = array();

        if (array_key_exists(VALUE_LITERAL,$mongoValueObject))
        {
            // only allow valid values
            if($this->isValidLiteral($mongoValueObject[VALUE_LITERAL])){
                // single value literal
                $simpleGraphValueObject[] = array(
                    'type'=>'literal',
                    'value'=>$mongoValueObject[VALUE_LITERAL]);
            }
        }
        else if (array_key_exists(VALUE_URI,$mongoValueObject))
        {
            // only allow valid values
            if($this->isValidResource($mongoValueObject[VALUE_URI])) {
                // single value uri
                $simpleGraphValueObject[] = array(
                    'type' => 'uri',
                    'value' => $this->_labeller->qname_to_alias($mongoValueObject[VALUE_URI]));
            }
        }
        else
        {
            // If we have an array of values
            foreach ($mongoValueObject as $kvp)
            {
                foreach ($kvp as $type=>$value)
                {
                    // Make sure the value is valid
                    if($type==VALUE_LITERAL){
                        if(!$this->isValidLiteral($value)){
                            continue;
                        }
                        $valueTypeLabel = 'literal';
                    }
                    else{
                        if(!$this->isValidResource($value)){
                            continue;
                        }
                        $valueTypeLabel = 'uri';
                    }
                    $simpleGraphValueObject[] = array(
                        'type'=>$valueTypeLabel,
                        'value'=>($type==VALUE_URI) ? $this->_labeller->qname_to_alias($value) : $value);
                }
            }
        }
        // Otherwise we have found valid values
        return $simpleGraphValueObject;
    }

    /**
     * Convert from ExtendedGraph value object format (verbose) to Tripod format (compact)
     * @param array $simpleGraphValueObject
     * @return array
     */
    private function toMongoTripodValueObject($simpleGraphValueObject)
    {
        $valueTypeProp = ($simpleGraphValueObject['type']=="literal") ? VALUE_LITERAL : VALUE_URI;
        return array(
            $valueTypeProp=>
            ($simpleGraphValueObject['type']=="literal") ? $simpleGraphValueObject['value'] : $this->_labeller->uri_to_alias($simpleGraphValueObject['value']));
    }

    /**
     * @param \Tripod\ExtendedGraph|null $graph
     * @param string $contextAlias
     * @return array|null
     */
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
