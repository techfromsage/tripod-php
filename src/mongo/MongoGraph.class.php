<?php
require_once TRIPOD_DIR.'classes/ExtendedGraph.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodLabeller.class.php';
require_once TRIPOD_DIR . 'mongo/serializers/MongoTripodNQuadSerializer.class.php';

/**
 * - set_index
 */
class MongoGraph extends ExtendedGraph {

    /**
     * {@inheritdoc}
     */
    public function __construct($graph=false)
    {
        $this->_labeller = new MongoTripodLabeller();
        if($graph){
            if(is_string($graph)){
                $this->add_rdf($graph);
            } else if ($graph instanceof ExtendedGraph) {
                $this->set_index($graph->get_index());
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function get_index($s=null,$p=null) {
        $sAlias = $this->_labeller->uri_to_alias($s);
        $pAlias = $this->_labeller->uri_to_alias($p);

        if ($s==null)
        {
            return $this->qnames_to_aliases(parent::get_index());
        }
        else if ($p==null)
        {
            $index = $this->qnames_to_aliases(array($sAlias=>parent::get_index($sAlias)));
            return (empty($index[$s])) ? array() : $index[$s];
        }
        else
        {
            $index = $this->qnames_to_aliases(array($sAlias=>array($pAlias=>parent::get_index($sAlias,$pAlias))));
            return (empty($index[$s][$p])) ? array() : $index[$s][$p];
        }
    }

    /**
     * {@inheritdoc}
     */
    public function get_subjects()
    {
        $subjects = array();
        $subjectQnames = array_keys($this->_index);
        foreach ($subjectQnames as $qname) {
            $subjects[] = $this->_labeller->qname_to_alias($qname);
        }
        return $subjects;
    }

    /**
     * {@inheritdoc}
     */
    public function set_index($_i)
    {
        parent::set_index($this->uris_to_aliases($_i));
    }


    /**
     * {@inheritdoc}
     */
    public function remove_resource_triple( $s, $p, $o) {
        $s = $this->_labeller->uri_to_alias($s);
        $p = $this->_labeller->uri_to_alias($p);
        $o = $this->_labeller->uri_to_alias($o);
        parent::remove_resource_triple($s,$p,$o);
    }

    /**
     * {@inheritdoc}
     */
    public function remove_literal_triple( $s, $p, $o) {
        $s = $this->_labeller->uri_to_alias($s);
        $p = $this->_labeller->uri_to_alias($p);
        parent::remove_literal_triple($s, $p, $o);
    }

    /**
     * {@inheritdoc}
     */
    public function remove_property_values($s, $p) {
        $s = $this->_labeller->uri_to_alias($s);
        $p = $this->_labeller->uri_to_alias($p);
        parent::remove_property_values($s, $p);
    }

    /**
     * {@inheritdoc}
     */
    public function remove_triples_about($s) {
        $s = $this->_labeller->uri_to_alias($s);
        parent::remove_triples_about($s);
    }


    /**
     * {@inheritdoc}
     */
    protected function add_triple($s, $p, $o_info)
    {
        $s = $this->_labeller->uri_to_alias($s);
        $p = $this->_labeller->uri_to_alias($p);
        if (is_array($o_info) && array_key_exists("type",$o_info) && $o_info["type"]=="uri")
        {
            $o_info["value"] = $this->_labeller->uri_to_alias($o_info["value"]);
        }
        parent::add_triple($s, $p, $o_info);
    }

    /**
     * Given a context this method serializes the current graph to nquads of the form
     *  <s> <p> <o> <context> .
     * @param string $context the context for the graph your are serializing
     * @return string the nquad serialization of the graph
     * @throws InvalidArgumentException if you do not specify a context
     */
    public function to_nquads($context)
    {
        if(empty($context)) {
            throw new InvalidArgumentException("You must specify the context when serializing to nquads");
        }

        $serializer = new MongoTripodNQuadSerializer();
        return $serializer->getSerializedIndex($this->get_index(), $this->_labeller->qname_to_alias($context));
    }

    /**
     * Adds the tripod array(s) to this graph.
     * This method is used to add individual tripod documents, or a series of tripod array documents that are embedded in a view.
     * @param $tarray
     * @throws TripodException
     */
    public function add_tripod_array($tarray)
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
    public function to_tripod_array($docId,$context)
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
    public function to_tripod_view_array($docId,$context)
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
                $predObjects[$key] = $this->toGraphValueObject($value);
            }
        }
        $_i[$tarray["_id"][_ID_RESOURCE]] = $predObjects;
        $this->_index = $this->merge($this->_index, $_i);
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
                'value'=>$mongoValueObject[VALUE_URI]);
        }
        else
        {
            foreach ($mongoValueObject as $kvp)
            {
                foreach ($kvp as $type=>$value)
                {
                    $simpleGraphValueObject[] = array(
                        'type'=>($type==VALUE_LITERAL) ? 'literal' : 'uri',
                        'value'=>$value);
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
            $valueTypeProp=> $simpleGraphValueObject['value']);
    }

    /**
     * Take a graph's index and convert it to a tarray
     * @param null $graph
     * @param $contextAlias
     * @return array|null
     */
    private function index_to_tarray($graph=null,$contextAlias)
    {
        if ($graph==null) {
            $graph = $this;
        } else {
            $graph = new MongoGraph($graph); // make sure it's a mongo graph
        }
        $_i = $graph->_index;

        foreach ($_i as $resource=>$predObjects)
        {
            $doc = array();
            $id = array();

            $id[_ID_RESOURCE] = $resource;
            $id[_ID_CONTEXT] = $contextAlias;
            $doc["_id"] = $id;
            foreach ($predObjects as $pQName=>$objects)
            {
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

    /**
     * Move through the triple index and convert qnames to uris where they
     * @param array $index
     * @return array
     */
    private function qnames_to_aliases(array $index)
    {
        if (empty($index))
        {
            return array();
        }

        $result = array();
        foreach ($index as $subject=>$predicateObjects)
        {
            $subject = $this->_labeller->qname_to_alias($subject);
            if (is_array($predicateObjects))
            {
                foreach ($predicateObjects as $predicate=>$objects)
                {
                    $predicate = $this->_labeller->qname_to_alias($predicate);
                    if (is_array($objects))
                    {
                        foreach($objects as $object)
                        {
                            // this is a value object
                            if ($object['type']==='uri')
                            {
                                $object['value'] = $this->_labeller->qname_to_alias($object['value']);
                            }
                            if (!isset($result[$subject]))
                            {
                                $result[$subject] = array();
                            }
                            if (!isset($result[$subject][$predicate]))
                            {
                                $result[$subject][$predicate] = array();
                            }
                            $result[$subject][$predicate][] = $object;
                        }
                    }
                }
            }
        }
        return $result;
    }

    private function uris_to_aliases(array $index)
    {
        $result = array();
        foreach ($index as $subject=>$predicateObjects)
        {
            $subject = $this->_labeller->uri_to_alias($subject);
            if (is_array($predicateObjects))
            {
                foreach ($predicateObjects as $predicate=>$objects)
                {
                    $predicate = $this->_labeller->uri_to_alias($predicate);
                    if (is_array($objects))
                    {
                        foreach($objects as $object)
                        {
                            // this is a value object
                            if ($object['type']==='uri')
                            {
                                $object['value'] = $this->_labeller->uri_to_alias($object['value']);
                            }
                            if (!isset($result[$subject]))
                            {
                                $result[$subject] = array();
                            }
                            if (!isset($result[$subject][$predicate]))
                            {
                                $result[$subject][$predicate] = array();
                            }
                            $result[$subject][$predicate][] = $object;
                        }
                    }
                }
            }
        }
        return $result;
    }
}