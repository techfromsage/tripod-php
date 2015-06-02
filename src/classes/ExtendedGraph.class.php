<?php

require_once(ARC_DIR.'ARC2.php');
/**
 * This class is based on SimpleGraph, part of Moriaty: https://code.google.com/p/moriarty/
 * @see https://code.google.com/p/moriarty/source/browse/trunk/labeller.class.php
 */
class ExtendedGraph
{
    /* FROM SimpleGraph */
    var $_index = array();
    var $_image_properties =  array( 'http://xmlns.com/foaf/0.1/depiction', 'http://xmlns.com/foaf/0.1/img');
    var $_property_order =  array('http://www.w3.org/2004/02/skos/core#prefLabel', RDFS_LABEL, 'http://purl.org/dc/terms/title', DC_TITLE, FOAF_NAME, 'http://www.w3.org/2004/02/skos/core#definition', RDFS_COMMENT, 'http://purl.org/dc/terms/description', DC_DESCRIPTION, 'http://purl.org/vocab/bio/0.1/olb', RDF_TYPE);
    var $parser_errors = array();
    protected $_ns = array (
        'rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs' => 'http://www.w3.org/2000/01/rdf-schema#',
        'owl' => 'http://www.w3.org/2002/07/owl#',
        'cs' => 'http://purl.org/vocab/changeset/schema#',
        'bf' => 'http://schemas.talis.com/2006/bigfoot/configuration#',
        'frm' => 'http://schemas.talis.com/2006/frame/schema#',

        'dc' => 'http://purl.org/dc/elements/1.1/',
        'dct' => 'http://purl.org/dc/terms/',
        'dctype' => 'http://purl.org/dc/dcmitype/',
        'geo' => 'http://www.w3.org/2003/01/geo/wgs84_pos#',
        'rel' => 'http://purl.org/vocab/relationship/',
        'wn' => 'http://xmlns.com/wordnet/1.6/',
        'air' => 'http://www.daml.org/2001/10/html/airport-ont#',
        'contact' => 'http://www.w3.org/2000/10/swap/pim/contact#',
        'frbr' => 'http://purl.org/vocab/frbr/core#',

        'ad' => 'http://schemas.talis.com/2005/address/schema#',
        'lib' => 'http://schemas.talis.com/2005/library/schema#',
        'dir' => 'http://schemas.talis.com/2005/dir/schema#',
        'user' => 'http://schemas.talis.com/2005/user/schema#',
        'sv' => 'http://schemas.talis.com/2005/service/schema#',
        'mo' => 'http://purl.org/ontology/mo/',
        'status' => 'http://www.w3.org/2003/06/sw-vocab-status/ns#',
        'label' => 'http://purl.org/net/vocab/2004/03/label#',
        'skos' => 'http://www.w3.org/2004/02/skos/core#',
        'bibo' => 'http://purl.org/ontology/bibo/',
        'ov' => 'http://open.vocab.org/terms/',
        'foaf' => 'http://xmlns.com/foaf/0.1/',
        'void' => 'http://rdfs.org/ns/void#',
        'xsd' => 'http://www.w3.org/2001/XMLSchema#',
    );

    var $_labeller;

    public function __construct($graph=false){
        $this->_labeller = new Labeller();
        if($graph){
            if(is_string($graph)){
                $this->add_rdf($graph);
            } else {
                $this->set_index($graph);
            }
        }

    }

    public function __destruct(){
        unset($this->_index);
        unset($this);
    }


    /**
     * Map a portion of a URI to a short prefix for use when serialising the graph
     * @param string prefix the namespace prefix to associate with the URI
     * @param string uri the URI to associate with the prefix
     */
    public function set_namespace_mapping($prefix, $uri) {
        $this->_labeller->set_namespace_mapping($prefix, $uri);
    }

    /**
     * Convert a QName to a URI using registered namespace prefixes
     * @param string qname the QName to convert
     * @return string the URI corresponding to the QName if a suitable prefix exists, null otherwise
     */
    public function qname_to_uri($qname) {
        return $this->_labeller->qname_to_uri($qname);
    }

    /**
     * Convert a URI to a QName using registered namespace prefixes
     * @param string uri the URI to convert
     * @return string the QName corresponding to the URI if a suitable prefix exists, null otherwise
     */
    public function uri_to_qname($uri) {
        return $this->_labeller->uri_to_qname($uri);
    }

    public function get_prefix($ns) {
        return $this->_labeller->get_prefix($ns);
    }

    public function add_labelling_property($p)  {
        $this->_labeller->add_labelling_property($p);
    }

    // todo: not clear this actually does anything
    public function update_prefix_mappings() {
        foreach ($this->get_index() as $s => $p_list) {
            foreach ($p_list as $p => $v_list) {
                $prefix = $this->_labeller->uri_to_qname($p);
            }
        }
    }

    /**
     * Constructs an array containing the type of the resource and its value
     * @param string resource a URI or blank node identifier (prefixed with _: e.g. _:name)
     * @return array an associative array with two keys: 'type' and 'value'. Type is either bnode or uri
     */
    public function make_resource_array($resource) {
        $resource_type = strpos($resource, '_:' ) === 0 ? 'bnode' : 'uri';
        return array('type' => $resource_type, 'value' => $resource);
    }

    /**
     * Adds a triple with a resource object to the graph
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     * @param string o the object of the triple, either a URI or a blank node in the format _:name
     * @return boolean true if the triple was new, false if it already existed in the graph
     */
    public function add_resource_triple($s, $p, $o) {
        return $this->add_triple($s, $p, array('type' => strpos($o, '_:' ) === 0 ? 'bnode' : 'uri', 'value' => $o));
    }

    /**
     * Adds a triple with a literal object to the graph
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate of the triple as a URI
     * @param string o the object of the triple as a string
     * @param string lang the language code of the triple's object (optional)
     * @param string dt the datatype URI of the triple's object (optional)
     * @return boolean true if the triple was new, false if it already existed in the graph
     */
    public function add_literal_triple($s, $p, $o, $lang = null, $dt = null) {
        $o_info = array('type' => 'literal', 'value' => $o);
        if ( $lang != null ) {
            $o_info['lang'] = $lang;
        }
        if ( $dt != null ) {
            $o_info['datatype'] = $dt;
        }
        return $this->add_triple($s, $p, $o_info);
    }

    protected function add_triple($s, $p, $o_info) {
        if (!isset($this->_index[$s])) {
            $this->_index[$s] = array();
            $this->_index[$s][$p] = array( $o_info );
            return true;
        }
        elseif (!isset($this->_index[$s][$p])) {
            $this->_index[$s][$p] = array( $o_info);
            return true;
        }
        else {
            if ( ! in_array( $o_info, $this->_index[$s][$p] ) ) {
                $this->_index[$s][$p][] = $o_info;
                return true;
            }
        }
        return false;
    }

    /**
     * @deprecated this is deprecated
     */
    public function get_triples() {
        return ARC2::getTriplesFromIndex($this->to_arc_index($this->get_index()));
    }

    /**
     * Get a copy of the graph's triple index
     * @param string $s delve into the index at $s; or return empty array
     * @param string $p delive into the index at $s, $p; or return empty array
     * @see http://n2.talis.com/wiki/RDF_PHP_Specification
     * @return array
     */
    public function get_index($s=null,$p=null) {
        if ($s===null)
        {
            return $this->_index;
        }
        else if ($p===null)
        {
            if (isset($this->_index[$s]))
            {
                return $this->_index[$s];
            }
            return array();
        }
        else
        {
            if (isset($this->_index[$s]) && isset($this->_index[$s][$p]))
            {
                return $this->_index[$s][$p];
            }
            return array();
        }
    }

    public function set_index($_i)
    {
        $this->_index = $_i;
    }


    /**
     * Serialise the graph to RDF/XML
     * @return string the RDF/XML version of the graph
     */
    public function to_rdfxml() {
        $this->update_prefix_mappings();
        $serializer = ARC2::getRDFXMLSerializer(
            array(
                'ns' => $this->_labeller->get_ns(),
            )
        );
        return $serializer->getSerializedIndex($this->to_arc_index($this->get_index()));
    }

    /**
     * Serialise the graph to Turtle
     * @see http://www.dajobe.org/2004/01/turtle/
     * @return string the Turtle version of the graph
     */
    public function to_turtle() {
        $this->update_prefix_mappings();
        $serializer = ARC2::getTurtleSerializer(
            array(
                'ns' => $this->_labeller->get_ns(),
            )
        );
        return $serializer->getSerializedIndex($this->to_arc_index($this->get_index()));
    }

    /**
     * Serialise the graph to N-Triples
     * @see http://www.w3.org/TR/rdf-testcases/#ntriples
     * @return string the N-Triples version of the graph
     */
    public function to_ntriples() {
        $serializer = ARC2::getComponent('NTriplesSerializer', array());
        return $serializer->getSerializedIndex($this->to_arc_index($this->get_index()));
    }


    /**
     * Serialise the graph to JSON
     * @see http://n2.talis.com/wiki/RDF_JSON_Specification
     * @return string the JSON version of the graph
     */
    public function to_json() {
        return json_encode($this->get_index());
    }


    /**
     * Fetch the first literal value for a given subject and predicate. If there are multiple possible values then one is selected at random.
     * @param string s the subject to search for
     * @param string p the predicate to search for, or an array of predicates
     * @param string default a default value to use if no literal values are found
     * @return string the first literal value found or the supplied default if no values were found
     */
    public function get_first_literal($s, $p, $default = null, $preferred_language = null) {

        $best_literal = $default;
        if ( array_key_exists($s, $this->_index)) {
            if (is_array($p)) {
                foreach($p as $p_uri) {
                    foreach ($this->get_index($s,$p_uri) as $value) {
                        if ($value['type'] == 'literal') {
                            if ($preferred_language == null) {
                                return $value['value'];
                            }
                            else {
                                if (array_key_exists('lang', $value) && $value['lang'] == $preferred_language) {
                                    return $value['value'];
                                }
                                else {
                                    $best_literal = $value['value'];
                                }
                            }
                        }
                    }
                }
            }
            else {
                foreach ($this->get_index($s,$p) as $value) {
                    if ($value['type'] == 'literal') {
                        if ($preferred_language == null) {
                            return $value['value'];
                        }
                        else {
                            if (array_key_exists('lang', $value) && $value['lang'] == $preferred_language) {
                                return $value['value'];
                            }
                            else {
                                $best_literal = $value['value'];
                            }
                        }
                    }
                }
            }
        }

        return $best_literal;
    }

    /**
     * Fetch the first resource value for a given subject and predicate. If there are multiple possible values then one is selected at random.
     * @param string s the subject to search for
     * @param string p the predicate to search for
     * @param string default a default value to use if no literal values are found
     * @return string the first resource value found or the supplied default if no values were found
     */
    public function get_first_resource($s, $p, $default = null) {
        foreach ($this->get_index($s,$p) as $value) {
            if ($value['type'] == 'uri' || $value['type'] == 'bnode' ) {
                return $value['value'];
            }
        }
        return $default;
    }

    /**
     * Remove a triple with a resource object from the graph
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     * @param string o the object of the triple, either a URI or a blank node in the format _:name
     */
    public function remove_resource_triple( $s, $p, $o) {
        for ($i = count($this->_index[$s][$p]) - 1; $i >= 0; $i--) {
            if (($this->_index[$s][$p][$i]['type'] == 'uri' || $this->_index[$s][$p][$i]['type'] == 'bnode') && $this->_index[$s][$p][$i]['value'] == $o)  {
                array_splice($this->_index[$s][$p], $i, 1);
            }
        }

        if (count($this->_index[$s][$p]) == 0) {
            unset($this->_index[$s][$p]);
        }
        if (count($this->_index[$s]) == 0) {
            unset($this->_index[$s]);
        }

    }

    public function remove_literal_triple( $s, $p, $o) {
        for ($i = count($this->_index[$s][$p]) - 1; $i >= 0; $i--) {
            if ($this->_index[$s][$p][$i]['type'] == 'literal' && $this->_index[$s][$p][$i]['value'] == $o)  {
                array_splice($this->_index[$s][$p], $i, 1);
            }
        }

        if (count($this->_index[$s][$p]) == 0) {
            unset($this->_index[$s][$p]);
        }
        if (count($this->_index[$s]) == 0) {
            unset($this->_index[$s]);
        }

    }

    /**
     * Remove all triples having the supplied subject
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     */
    public function remove_triples_about($s) {
        unset($this->_index[$s]);
    }


    /**
     * Replace the triples in the graph with those parsed from the supplied RDF/XML
     * @param string rdfxml the RDF/XML to parse
     * @param string base the base URI against which relative URIs in the RDF/XML document will be resolved
     */
    public function from_rdfxml($rdfxml, $base='') {
        if ($rdfxml) {
            $this->remove_all_triples();
            $this->add_rdfxml($rdfxml, $base);
        }
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied JSON
     * @see http://n2.talis.com/wiki/RDF_JSON_Specification
     * @param string json the JSON to parse
     */
    public function from_json($json) {
        if ($json) {
            $this->remove_all_triples();
            $this->_index = json_decode($json, true);
        }
    }


    /**
     * Add the triples parsed from the supplied JSON to the graph
     * @see http://n2.talis.com/wiki/RDF_JSON_Specification
     * @param string json the JSON to parse
     */
    public function add_json($json) {
        if ($json) {
            $json_index = json_decode($json, true);
            $this->_index = $this->merge($this->_index, $json_index);
        }
    }

    public function get_parser_errors(){
        return $this->parser_errors;
    }
    /**
     * Add the triples parsed from the supplied RDF to the graph - let ARC guess the input
     * @param string rdf the RDF to parse
     * @param string base the base URI against which relative URIs in the RDF document will be resolved
     * @author Keith Alexander
     */
    public function add_rdf($rdf=false, $base='') {
        if ($rdf) {
            $trimRdf = trim($rdf);
            if($trimRdf[0]=='{'){ //lazy is-this-json assessment  - might be better to try json_decode - but more costly
                $this->add_json($trimRdf);
                unset($trimRdf);
            } else {
                $parser = ARC2::getRDFParser();
                $parser->parse($base, $rdf);
                $errors = $parser->getErrors();
                if(!empty($errors)){
                    $this->parser_errors[]=$errors;
                }
                $this->add_arc2_triple_list($parser->getTriples());
                unset($parser);
            }
        }
    }

    /**
     * Add the triples parsed from the supplied RDF/XML to the graph
     * @param string rdfxml the RDF/XML to parse
     * @param string base the base URI against which relative URIs in the RDF/XML document will be resolved
     */
    public function add_rdfxml($rdfxml, $base='') {
        if ($rdfxml) {
            $parser = ARC2::getRDFXMLParser();
            $parser->parse($base, $rdfxml );
            $this->add_arc2_triple_list($parser->getTriples());
            unset($parser);
        }
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied Turtle
     * @see http://www.dajobe.org/2004/01/turtle/
     * @param string turtle the Turtle to parse
     * @param string base the base URI against which relative URIs in the Turtle document will be resolved
     */
    public function from_turtle($turtle, $base='') {
        if ($turtle) {
            $this->remove_all_triples();
            $this->add_turtle($turtle, $base);
        }
    }

    /**
     * Add the triples parsed from the supplied Turtle to the graph
     * @see http://www.dajobe.org/2004/01/turtle/
     * @param string turtle the Turtle to parse
     * @param string base the base URI against which relative URIs in the Turtle document will be resolved
     */
    public function add_turtle($turtle, $base='') {
        if ($turtle) {
            $parser = ARC2::getTurtleParser();
            $parser->parse($base, $turtle );
            $this->add_arc2_triple_list($parser->getTriples());
            unset($parser);
        }
    }


    /**
     * Add the triples in the supplied graph to the current graph
     * @param ExtendedGraph g the graph to read
     */
    public function add_graph(ExtendedGraph $g) {
        $triples_were_added = false;
        $index = $g->get_index();
        foreach ($index as $s => $p_list) {
            foreach ($p_list as $p => $o_list) {
                foreach ($o_list as $o_info) {
                    if ($this->add_triple($s, $p, $o_info) ) {
                        $triples_were_added = true;
                    }
                }
            }
        }
        return $triples_were_added;
    }


    private function add_arc2_triple_list(&$triples) {
        $bnode_index = array();

        // We can safely preserve bnode labels if the graph is empty, otherwise we need to rewrite them
        $rewrite_bnode_labels = $this->is_empty() ? FALSE : TRUE;

        foreach ($triples as $t) {
            $obj = array();

            if ($rewrite_bnode_labels && $t['o_type'] == 'bnode') {
                if (!array_key_exists($t['o'], $bnode_index)) {
                    $bnode_index[$t['o']] = uniqid('_:mor');
                }
                $obj['value'] = $bnode_index[$t['o']];
            }
            else {
                $obj['value'] = $t['o'];
            }

            if ($rewrite_bnode_labels && strpos($t['s'], '_:' ) === 0) {
                if (!array_key_exists($t['s'], $bnode_index)) {
                    $bnode_index[$t['s']] = uniqid('_:mor');
                }
                $t['s'] = $bnode_index[$t['s']];
            }


            if ($t['o_type'] === 'iri' ) {
                $obj['type'] = 'uri';
            }
            elseif ($t['o_type'] === 'literal1' ||
                $t['o_type'] === 'literal2' ||
                $t['o_type'] === 'long_literal1' ||
                $t['o_type'] === 'long_literal2'
            ) {
                $obj['type'] = 'literal';
            }
            else {
                $obj['type'] = $t['o_type'];
            }



            if ($obj['type'] == 'literal') {
                if ( isset( $t['o_dt'] ) && $t['o_dt'] ) {
                    $obj['datatype'] = $t['o_dt'];
                }
                else if ( isset( $t['o_datatype'] ) && $t['o_datatype'] ) {
                    $obj['datatype'] = $t['o_datatype'];
                }
                if ( isset( $t['o_lang']) && $t['o_lang'])  {
                    $obj['lang'] = $t['o_lang'];
                }
            }

            if (!isset($this->_index[$t['s']])) {
                $this->_index[$t['s']] = array();
                $this->_index[$t['s']][$t['p']] = array($obj);
            }
            elseif (!isset($this->_index[$t['s']][$t['p']])) {
                $this->_index[$t['s']][$t['p']] = array($obj);
            }
            else {
                if ( ! in_array( $obj, $this->_index[$t['s']][$t['p']] ) ) {
                    $this->_index[$t['s']][$t['p']][] = $obj;
                }
            }
        }
    }


    // until ARC2 upgrades to support RDF/PHP we need to rename all types of "uri" to "iri"
    private function to_arc_index(&$index) {
        $ret = array();

        foreach ($index as $s => $s_info) {
            $ret[$s] = array();
            foreach ($s_info as $p => $p_info) {
                $ret[$s][$p] = array();
                foreach ($p_info as $o) {
                    $o_new = array();
                    foreach ($o as $key => $value) {
                        if ( $key == 'type' && $value == 'uri' ) {
                            $o_new['type'] = 'iri';
                        }
                        else {
                            $o_new[$key] = $value;
                        }
                    }
                    $ret[$s][$p][] = $o_new;
                }
            }
        }
        return $ret;
    }

    /**
     * Tests whether the graph contains the given triple
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     * @param string o the object of the triple, either a URI or a blank node in the format _:name
     * @return boolean true if the triple exists in the graph, false otherwise
     */
    public function has_resource_triple($s, $p, $o) {
        foreach ($this->get_index($s,$p) as $value) {
            if ( ( $value['type'] == 'uri' || $value['type'] == 'bnode') && $value['value'] === $o) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tests whether the graph contains the given triple
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     * @param string o the object of the triple as a literal value
     * @return boolean true if the triple exists in the graph, false otherwise
     */
    public function has_literal_triple($s, $p, $o, $lang = null, $dt = null) {
        foreach ($this->get_index($s,$p) as $value) {
            if ( ( $value['type'] == 'literal') && $value['value'] === $o) {

                if ($lang !== null) {
                    return (array_key_exists('lang', $value) && $value['lang'] === $lang);
                }

                if ($dt !== null) {
                    return (array_key_exists('datatype', $value) && $value['datatype'] === $dt);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Fetch the resource values for a given subject and predicate.
     * @param string s the subject to search for
     * @param string p the predicate to search for
     * @return array list of URIs and blank nodes that are the objects of triples with the supplied subject and predicate
     */
    public function get_resource_triple_values($s, $p) {
        $values = array();
        foreach ($this->get_index($s,$p) as $value) {
            if ( ( $value['type'] == 'uri' || $value['type'] == 'bnode')) {
                $values[] = $value['value'];
            }
        }
        return $values;
    }

    /**
     * Fetch the literal values for a given subject and predicate.
     * @param string s the subject to search for
     * @param string p the predicate to search for
     * @return array list of literals that are the objects of triples with the supplied subject and predicate
     */
    public function get_literal_triple_values($s, $p) {
        $values = array();
        if ( array_key_exists($s, $this->_index)) {
            if (is_array($p)) {
                foreach($p as $p_uri) {
                    foreach ($this->get_index($s,$p_uri) as $value) {
                        if ($value['type'] == 'literal') {
                            $values[] = $value['value'];
                        }
                    }
                }
            }
            else {
                foreach ($this->get_index($s,$p) as $value) {
                    if ($value['type'] == 'literal') {
                        $values[] = $value['value'];
                    }
                }
            }
        }

        return $values;
    }


    /**
     * Fetch the values for a given subject and predicate.
     * @param string s the subject to search for
     * @param string p the predicate to search for
     * @return array list of values of triples with the supplied subject and predicate
     */
    public function get_subject_property_values($s, $p) {
        $values = array();
        if (! is_array($p)) $p = array($p);
        if (array_key_exists($s, $this->_index) ) {
            foreach ($p as $pinst) {
                foreach ($this->get_index($s,$pinst) as $value) {
                    $values[] = $value;
                }
            }
        }
        return $values;
    }

    /**
     * Fetch a subgraph where all triples have given subject
     * @param string s the subject to search for
     * @return ExtendedGraph triples with the supplied subject
     */
    public function get_subject_subgraph($s) {
        $sub = new ExtendedGraph();
        if (array_key_exists($s, $this->_index) ) {
            $sub->_index[$s] = $this->get_index($s);
        }
        return $sub;
    }

    /**
     * Fetch an array of all the subjects
     * @return array
     */
    public function get_subjects() {
        return array_keys($this->_index);
    }


    /**
     * Fetch an array of all the subject that have and rdf type that matches that given
     * @param $t the type to match
     * @return array
     */
    public function get_subjects_of_type($t) {
        return $this->get_subjects_where_resource('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', $t);
    }

    /**
     * Fetch an array of all the subjects where the predicate and object match a ?s $p $o triple in the graph and the object is a resource
     * @param $p the predicate to match
     * @param $o the resource object to match
     * @return array
     */
    public function get_subjects_where_resource($p, $o) {
        return array_merge($this->get_subjects_where($p, $o, 'uri'), $this->get_subjects_where($p, $o, 'bnode'));
    }

    /**
     * Fetch an array of all the subjects where the predicate and object match a ?s $p $o triple in the graph and the object is a literal value
     * @param $p the predicate to match
     * @param $o the resource object to match
     * @return array
     */
    public function get_subjects_where_literal($p, $o) {
        return $this->get_subjects_where($p, $o, 'literal');
    }

    private function get_subjects_where($p, $o, $type)
    {
        $subjects = array();
        foreach ($this->_index as $subject => $properties)
        {
            if (array_key_exists($p, $properties))
            {
                foreach ($properties[$p] as $object)
                {
                    if ($object['type'] == $type && $object['value'] == $o)
                    {
                        $subjects[] = $subject;
                        break;
                    }
                }
            }
        }
        return $subjects;
    }

    /**
     * Fetch the properties of a given subject and predicate.
     * @param string s the subject to search for
     * @param boolean distinct if true then duplicate properties are included only once (optional, default is true)
     * @return array list of property URIs
     */
    public function get_subject_properties($s, $distinct = TRUE) {
        $values = array();
        foreach ($this->get_index($s) as $prop => $prop_values ) {
            if ($distinct) {
                $values[] = $prop;
            }
            else {
                for ($i = 0; $i < count($prop_values); $i++) {
                    $values[] = $prop;
                }
            }
        }
        return $values;
    }


    /**
     * Tests whether the graph contains a triple with the given subject and predicate
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     * @return boolean true if a matching triple exists in the graph, false otherwise
     */
    public function subject_has_property($s, $p) {
        $values = $this->get_index($s,$p);
        return !empty($values);
    }

    /**
     * Tests whether the graph contains a triple with the given subject
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @return boolean true if the graph contains any triples with the specified subject, false otherwise
     */
    public function has_triples_about($s) {
        $predicates = $this->get_index($s);
        return !empty($predicates);
    }


    /**
     * Removes all triples with the given subject and predicate
     * @param string s the subject of the triple, either a URI or a blank node in the format _:name
     * @param string p the predicate URI of the triple
     */
    public function remove_property_values($s, $p) {
        unset($this->_index[$s][$p]);
    }

    /**
     * Clears all triples out of the graph
     */
    public function remove_all_triples() {
        $this->_index = array();
    }

    /**
     * Tests whether the graph contains any triples
     * @return boolean true if the graph contains no triples, false otherwise
     */
    public function is_empty() {
        return ( count($this->_index) == 0);
    }


    public function get_label($resource_uri, $capitalize = false, $use_qnames = FALSE) {
        return $this->_labeller->get_label($resource_uri, $this, $capitalize, $use_qnames);
    }

    public function get_inverse_label($resource_uri, $capitalize = false, $use_qnames = FALSE) {
        return $this->_labeller->get_inverse_label($resource_uri, $this, $capitalize, $use_qnames);
    }

    public function get_description($resource_uri = null) {
        if ($resource_uri == null) {
            $resource_uri = $this->_primary_resource;
        }
        $text = $this->get_first_literal($resource_uri,'http://purl.org/dc/terms/description', '', 'en');
        if ( strlen($text) == 0) {
            $text = $this->get_first_literal($resource_uri,DC_DESCRIPTION, '', 'en');
        }
        if ( strlen($text) == 0) {
            $text = $this->get_first_literal($resource_uri,RDFS_COMMENT, '', 'en');
        }
        if ( strlen($text) == 0) {
            $text = $this->get_first_literal($resource_uri,'http://purl.org/rss/1.0/description', '', 'en');
        }
        if ( strlen($text) == 0) {
            $text = $this->get_first_literal($resource_uri,'http://purl.org/dc/terms/abstract', '', 'en');
        }
        if ( strlen($text) == 0) {
            $text = $this->get_first_literal($resource_uri,'http://purl.org/vocab/bio/0.1/olb', '', 'en');
        }
        return $text;
    }

    public function reify($resources, $nodeID_prefix='Statement')
    {
        $RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
        $reified = array();
        $statement_no = 1;
        foreach($resources as $uri => $properties){
            foreach($properties as $property => $objects){
                foreach($objects as $object){
                    while(!isset($statement_nodeID) OR isset($resources[$statement_nodeID]) OR isset($reified[$statement_nodeID]))
                    {
                        $statement_nodeID = '_:'.$nodeID_prefix.($statement_no++);
                    }
                    $reified[$statement_nodeID]= array(
                        $RDF.'type'=>array(
                            array('type'=>'uri','value'=>$RDF.'Statement')
                        ),
                        $RDF.'subject' => array(array('type'=>  (substr($uri,0,2)=='_:')? 'bnode' : 'uri', 'value'=>$uri)),
                        $RDF.'predicate' => array(array('type'=>'uri','value'=>$property)),
                        $RDF.'object' => array($object),
                    );

                }
            }
        }

        return ($reified);
    }

    /**
     * diff
     * returns a simpleIndex consisting of all the statements in array1 that weren't found in any of the subsequent arrays
     * @param array1, array2, [array3, ...]
     * @return array
     * @author Keith
     **/
    public function diff(){
        $indices = func_get_args();
        if(count($indices)==1){
            array_unshift($indices, $this->_index);
        }
        $base = array_shift($indices);
        if (count($base) === 0) return array();
        $diff = array();

        foreach($base as $base_uri => $base_ps) {
            foreach($indices as $index){
                if(!isset($index[$base_uri])) {
                    $diff[$base_uri] = $base_ps;
                }
                else {
                    foreach($base_ps as $base_p => $base_obs) {
                        if(!isset($index[$base_uri][$base_p])) {
                            $diff[$base_uri][$base_p] = $base_obs;
                        }
                        else {
                            foreach($base_obs as $base_o){
                                // because we want to enforce strict type check
                                // on in_array, we need to ensure that array keys
                                // are ordered the same
                                ksort($base_o);
                                $base_p_values = $index[$base_uri][$base_p];
                                foreach($base_p_values as &$v)
                                {
                                    ksort($v);
                                }
                                if(!in_array($base_o, $base_p_values, true)) {
                                    $diff[$base_uri][$base_p][]=$base_o;
                                }
                            }
                        }
                    }
                }
            }
        }

        return $diff;
    }

    /**
     * merge
     * merges all  rdf/json-style arrays passed as parameters
     * @param array1, array2, [array3, ...]
     * @return array
     * @author Keith
     **/

    public function merge(){

        $old_bnodeids = array();
        $indices = func_get_args();
        if(count($indices)==1){
            array_unshift($indices, $this->_index);
        }

        $current = array_shift($indices);
        foreach($indices as $newGraph)
        {
            foreach($newGraph as $uri => $properties)
            {
                /* Make sure that bnode ids don't overlap:
                _:a in g1 isn't the same as _:a in g2 */

                if(substr($uri,0,2)=='_:')//bnode
                {
                    $old_id = $uri;
                    $count = 1;

                    while(isset($current[$uri]) OR
                        ( $old_id!=$uri AND isset($newGraph[$uri]) )
                        OR isset($old_bnodeids[$uri])
                    )
                    {
                        $uri.=$count++;
                    }

                    if($old_id != $uri) $old_bnodeids[$old_id] = $uri;
                }

                if (isset($properties) && is_array($properties)) {
                    foreach($properties as $property => $objects)
                    {
                        foreach($objects as $object)
                        {
                            /* make sure that the new bnode is being used*/
                            if($object['type']=='bnode')
                            {
                                $bnode = $object['value'];

                                if(isset($old_bnodeids[$bnode])) $object['value'] = $old_bnodeids[$bnode];
                                else //bnode hasn't been transposed
                                {
                                    $old_bnode_id = $bnode;
                                    $count=1;
                                    while(isset($current[$bnode]) OR
                                        ( $object['value']!=$bnode AND isset($newGraph[$bnode]) )
                                        OR isset($old_bnodeids[$uri])
                                    )
                                    {
                                        $bnode.=$count++;
                                    }

                                    if($old_bnode_id!=$bnode) $old_bnodeids[$old_bnode_id] = $bnode;
                                    $object['value'] = $bnode;
                                }
                            }

                            if(!isset($current[$uri][$property]) OR !in_array($object, $current[$uri][$property]))
                            {
                                $current[$uri][$property][]=$object;
                            }
                        }
                    }
                }
            }
        }
        return $current;
    }

    public function replace_resource($look_for, $replace_with) {
        $remove_list_resources = array();
        $remove_list_literals = array();
        $add_list_resources = array();
        $add_list_literals = array();
        foreach ($this->_index as $s => $p_list) {
            if ($s == $look_for) {
                foreach ($p_list as $p => $o_list) {
                    if ($p == $look_for) {
                        foreach ($o_list as $o_info) {
                            if ($o_info['type'] == 'literal') {
                                $lang = array_key_exists('lang', $o_info) ? $o_info['lang'] : null;
                                $dt = array_key_exists('datatype', $o_info) ? $o_info['datatype'] : null;

                                $remove_list_literals[] = array($look_for, $look_for, $o_info['value'], $lang, $dt);
                                $add_list_literals[] = array($replace_with, $replace_with, $o_info['value'], $lang, $dt);
                            }
                            else  {
                                if ($o_info['value'] == $look_for) {
                                    $remove_list_resources[] = array($look_for, $look_for, $look_for);
                                    $add_list_resources[] = array($replace_with, $replace_with, $replace_with);
                                }
                                else {
                                    $remove_list_resources[] = array($look_for, $look_for, $o_info['value']);
                                    $add_list_resources[] = array($replace_with, $replace_with, $o_info['value']);
                                }
                            }
                        }
                    }
                    else {
                        foreach ($o_list as $o_info) {
                            if ($o_info['type'] == 'literal') {
                                $lang = array_key_exists('lang', $o_info) ? $o_info['lang'] : null;
                                $dt = array_key_exists('datatype', $o_info) ? $o_info['datatype'] : null;

                                $remove_list_literals[] = array($look_for, $p, $o_info['value'], $lang, $dt);
                                $add_list_literals[] = array($replace_with, $p, $o_info['value'], $lang, $dt);
                            }
                            else  {
                                if ($o_info['value'] == $look_for) {
                                    $remove_list_resources[] = array($look_for, $p, $look_for);
                                    $add_list_resources[] = array($replace_with, $p, $replace_with);
                                }
                                else {
                                    $remove_list_resources[] = array($look_for, $p, $o_info['value']);
                                    $add_list_resources[] = array($replace_with, $p, $o_info['value']);
                                }
                            }
                        }
                    }
                }
            }
            else {

                foreach ($p_list as $p => $o_list) {
                    if ($p == $look_for) {
                        foreach ($o_list as $o_info) {
                            if ($o_info['type'] == 'literal') {
                                $lang = array_key_exists('lang', $o_info) ? $o_info['lang'] : null;
                                $dt = array_key_exists('datatype', $o_info) ? $o_info['datatype'] : null;

                                $remove_list_literals[] = array($s, $look_for, $o_info['value'], $lang, $dt);
                                $add_list_literals[] = array($s, $replace_with, $o_info['value'], $lang, $dt);
                            }
                            else  {
                                if ($o_info['value'] == $look_for) {
                                    $remove_list_resources[] = array($s, $look_for, $look_for);
                                    $add_list_resources[] = array($s, $replace_with, $replace_with);
                                }
                                else {
                                    $remove_list_resources[] = array($s, $look_for, $o_info['value']);
                                    $add_list_resources[] = array($s, $replace_with, $o_info['value']);
                                }
                            }
                        }
                    }
                    else {
                        foreach ($o_list as $o_info) {
                            if ($o_info['type'] != 'literal' && $o_info['value'] == $look_for) {
                                $remove_list_resources[] = array($s, $p, $look_for);
                                $add_list_resources[] = array($s, $p, $replace_with);
                            }
                        }
                    }
                }
            }
        }

        foreach ($remove_list_resources as $t) {
            $this->remove_resource_triple($t[0], $t[1], $t[2]);
        }
        foreach ($add_list_resources as $t) {
            $this->add_resource_triple($t[0], $t[1], $t[2]);
        }

        foreach ($remove_list_literals as $t) {
            $this->remove_literal_triple($t[0], $t[1], $t[2], $t[3], $t[4]);
        }
        foreach ($add_list_literals as $t) {
            $this->add_literal_triple($t[0], $t[1], $t[2], $t[3], $t[4]);
        }

    }

    public function get_list_values($listUri) {
        $array = array();
        while(!empty($listUri) AND $listUri != RDF_NIL){
            $array[]=$this->get_first_resource($listUri, RDF_FIRST);
            $listUri = $this->get_first_resource($listUri, RDF_REST);
        }
        return $array;
    }

    /* END SimpleGraph */

    /* Modifications start here */

    const rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
    const rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
    const rdf_seq = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq';

    /**
     * Up to the application to decide what constitures the label properties for a given app
     * @var array
     */
    private static $labelProperties;

    public static function initProperties(Array $properties)
    {
    	if (array_key_exists('labelProperties', $properties))
        {
            self::$labelProperties = $properties['labelProperties'];
        }
    }

    /**
     * Replaces $uri1 with $uri2 in subject, predicate and object position
     */
    public function replace_uris($uri1, $uri2){
        $index = $this->get_index();
        if(isset($index[$uri1])){
            $index[$uri2] = $index[$uri1];
            unset($index[$uri1]);
        }

        foreach($index as $uri => $properties){
            foreach($properties as $property => $objects){
                if($property ==$uri1){
                    $index[$uri][$uri2] = $objects;
                    $property = $uri2;
                    unset($index[$uri][$uri1]);
                }
                foreach($objects as $i => $object){
                    if($object['value'] == $uri1 AND $object['type']!== 'literal'){
                        $index[$uri][$property][$i]['value'] = $uri2;
                    }
                }
            }
        }
        $this->_index = $index;
    }

    /**
     * Gets a count of triples matching the pattern $s, $p, $o
     * @param bool $s
     * @param bool $p
     * @param bool $o
     * @return int
     */
    public function get_triple_count($s=false, $p=false, $o=false){
        $index = $this->get_index();

        if (empty($index))
        {
            return 0;
        }
        else
        {
            $counter = 0;

            foreach($index as $uri => $properties)
            {
                if (($s && ($s == $uri)) || !$s)
                {
                    foreach ($properties as $property => $objects)
                    {
                        if (($p && ($p == $property)) || !$p)
                        {
                            foreach ($objects as $object)
                            {
                                if (($o && $o == $object['value']) || !$o)
                                {
                                    $counter++;
                                }
                            }
                        }
                    }
                }
            }

            return $counter;
        }
    }

    /**
     * Fetch all the resource values for all subjects.
     * @return array the resource values found
     */
    public function get_resources() {
        $resources = array();
        $subjects  = $this->get_subjects();
        foreach($subjects as $subject) {
            $resources[] = $subject;
            $resources = array_merge($resources, $this->get_resources_for_subject($subject));
        }
        return array_unique($resources);
    }

    /**
     * Fetch all the resource values for a given subject.
     * @param string s the subject to search for
     * @return array the resource values found
     */
    public function get_resources_for_subject($s) {
        $resources = array();
        foreach ($this->get_index($s) as $p => $values) {
            foreach ($values as $value) {
                if ($value['type'] == 'uri' || $value['type'] == 'bnode' ) {
                    $resources[] = $value['value'];
                }
            }
        }
        return array_unique($resources);
    }

    /**
     * Remove all properties for predicate $p
     * @param $p
     */
    public function remove_properties($p) {
        foreach($this->get_subjects() as $s){
            $this->remove_property_values($s, $p);
        }
    }

    /**
     * Get all values for predicate $p regardless of subject
     * @param $p
     * @return array
     */
    public function get_resource_properties($p) {
        $resources = array();
        foreach($this->get_subjects() as $s) {
            $properties  = $this->get_resource_triple_values($s, $p);
            $resources = array_merge($resources, $properties);
        }
        return $resources;
    }

    /**
     * Get all subjects with a value of $o for predicate $p
     * @param $p
     * @param $o
     * @return array
     */
    public function get_subjects_with_property_value($p, $o) {
        $subjects = array();
        foreach($this->get_subjects() as $s) {
            if($this->has_resource_triple($s, $p, $o) || $this->has_literal_triple($s, $p, $o)) {
                $subjects[] = $s;
            }
        }
        return $subjects;
    }

    /**
     * Get all values in sequence referred to by $sequenceUri
     * @param $sequenceUri
     * @return array
     */
    public function get_sequence_values($sequenceUri)
    {
        $triples = $this->get_index();
        $properties = array();

        if (isset($triples[$sequenceUri]))
        {
            foreach ($triples[$sequenceUri] as $property => $objects)
            {
                if (strpos($property, self::rdf.'_') !== false)
                {
                    $key = substr($property, strpos($property, '_') + 1  );
                    $value = $this->get_first_resource($sequenceUri, $property);

                    if (empty($value))
                    {
                        $value = $this->get_first_literal($sequenceUri, $property);
                    }

                    $properties[$key] = $value;
                }
            }

            ksort($properties, SORT_NUMERIC);
        }

        $values = array();

        foreach($properties as $key=>$value)
        {
            $values[] = $value;
        }

        return $values;
    }

    /**
     * Get the next uri in the sequence $sequenceUri
     * @param $sequenceUri
     * @return int
     */
    public function get_next_sequence($sequenceUri)
    {
        $values = $this->get_sequence_values($sequenceUri);

        return count($values)+1;
    }

    /**
     * Add a literal to a sequence
     * @param $s
     * @param $o
     */
    public function add_literal_to_sequence($s, $o)
    {
        $this->add_to_sequence($s, $o, 'literal');
    }

    /**
     * Remove a resource from a specified sequence and reindex the sequence to remove hte gap.
     *
     * @param $sequenceUri
     * @param $resourceValue
     */
    public function remove_resource_from_sequence($sequenceUri, $resourceValue)
    {
        $sequenceProperties = $this->get_subject_properties($sequenceUri);
        $sequenceValues = $this->get_sequence_values($sequenceUri);

        // Remove existing data
        foreach ($sequenceProperties as $sequenceProperty)
        {
            if (strpos($sequenceProperty, self::rdf.'_') !== false)
            {
                $sequencePropertyValue = $this->get_first_resource($sequenceUri, $sequenceProperty);
                $this->remove_resource_triple($sequenceUri, $sequenceProperty, $sequencePropertyValue);
            }
        }

        // Recreate the sequence with the correct indexing.
        foreach ($sequenceValues as $sequenceValue)
        {
            if ($sequenceValue != $resourceValue)
            {
                $this->add_resource_to_sequence($sequenceUri, $sequenceValue);
            }
        }
    }

    /**
     * Add resource to sequence,
     * @param $s
     * @param $o
     */
    public function add_resource_to_sequence($s, $o)
    {
        $this->add_to_sequence($s, $o);
    }

    /**
     * Add resource to sequence in a given position
     * @param $s
     * @param $o
     * @param $position
     */
    public function add_resource_to_sequence_in_position($s, $o, $position)
    {
        $sequenceValues = $this->get_sequence_values($s);

        if (empty($sequenceValues) || $position > count($sequenceValues))
        {
            $this->add_resource_to_sequence($s, $o);
        }
        else
        {
            array_splice($sequenceValues, $position-1, 1, array($o, $sequenceValues[$position-1]));

            $properties = $this->get_subject_properties($s);
            foreach ($properties as $p)
            {
                if (strpos($p, self::rdf.'_') !== false)
                {
                    $this->remove_property_values($s, $p);
                }
            }

            foreach ($sequenceValues as $value)
            {
                $this->add_resource_to_sequence($s, $value);
            }
        }
    }

    private function add_to_sequence($s, $o, $type = 'resource')
    {
        $sequenceValue = $this->get_next_sequence($s);
        $this->add_resource_triple($s, self::rdf_type, self::rdf_seq);

        if ($type == 'literal')
        {
            $this->add_literal_triple($s, self::rdf."_{$sequenceValue}", $o);
        }
        else
        {
            $this->add_resource_triple($s, self::rdf."_{$sequenceValue}", $o);
        }
    }

    /**
     * Replace a triple in the graph with a new value
     * @param $s
     * @param $p
     * @param $oOldValue
     * @param $oNewValue
     * @return bool
     */
    public function replace_literal_triple($s, $p, $oOldValue, $oNewValue)
    {
        if ($this->has_literal_triple($s, $p, $oOldValue))
        {
            $this->remove_literal_triple($s, $p, $oOldValue);
            $this->add_literal_triple($s, $p, $oNewValue);
            return TRUE;
        }
        else
        {
            return FALSE;
        }
    }

    /**
     * Remove all values of $s and $p and add $s $p $o where $o is a resource
     * @param $s
     * @param $p
     * @param $o
     */
    public function replace_resource_triples( $s, $p, $o) {
        if($this->subject_has_property($s, $p)) {
            $this->remove_property_values($s, $p);
        }
        if(!empty($o)) {
            $this->add_resource_triple($s, $p, $o);
        }
    }

    /**
     * Remove all values of $s and $p and add $s $p $o where $o is a literal
     * @param $s
     * @param $p
     * @param $o
     */
    public function replace_literal_triples( $s, $p, $o) {
        if($this->subject_has_property($s, $p)) {
            $this->remove_property_values($s, $p);
        }
        if(!empty($o)) {
            $this->add_literal_triple($s, $p, $o);
        }
    }

    /**
     * Get a label for a uri
     * @param $uri
     * @return string
     * @throws TripodException
     */
    public function get_label_for_uri($uri)
    {
        if(!isset($this->_index[$uri])){
            return '';
        }
        if (!isset(self::$labelProperties))
        {
            throw new TripodException('Please initialise ExtendedGraph::$labelProperties');
        }
        foreach(self::$labelProperties as $p){
            if(isset($this->_index[$uri][$p])){
                return $this->_index[$uri][$p][0]['value'];
            }
        }

        return '';
    }

    /**
     * Is this graph equal to $otherGraph?
     * @param $otherGraph
     * @return bool
     */
    public function is_equal_to($otherGraph)
    {
        $diffThisAndThat = ExtendedGraph::diff($this->get_index(), $otherGraph->get_index());
        $diffThatAndThis = ExtendedGraph::diff($otherGraph->get_index(), $this->get_index());

        return (empty($diffThisAndThat) && empty($diffThatAndThis));
    }

    /**
     * Remove all subjects of $type
     * @param $type
     */
    public function remove_subjects_of_type($type)
    {
        $subjects = $this->get_subjects_of_type($type);
        foreach ($subjects as $s)
        {
            $this->remove_triples_about($s);
        }
    }

    /**
     * Add a graph to this graph
     * @param $graph
     */
    public function from_graph($graph) {
        if ($graph) {
            $this->remove_all_triples();
            $this->add_graph($graph);
        }
    }
}