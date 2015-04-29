<?php

/**
 * Represents a changeset. Can be used to create a changeset based on the difference between two bounded descriptions. The descriptions must share the same subject URI.
 * Adapted from Moriarty's changeset
 * @see https://code.google.com/p/moriarty/source/browse/trunk/changeset.class.php
 */
class ChangeSet extends MongoGraph {

    protected $subjectOfChange;
    var $before = array();
    protected $before_rdfxml;
    var $after = array();
    protected $after_rdfxml;
    protected $createdDate = null;
    protected $creatorName = null;
    protected $changeReason = null;
    protected $has_changes = false;
    protected $cs_resource = null;
    protected $include_count = 0;

    /**
     * Create a new changeset. This will calculate the required additions and removals based on before and after versions of a bounded description. The args parameter is an associative array that may have the following fields:
     * <ul>
     *   <li><em>subjectOfChange</em> => a string representing the URI of the changeset's subject of change</li>
     *   <li><em>createdDate</em> => a string representing the date of the changeset</li>
     *   <li><em>creatorName</em> => a string representing the creator of the changeset</li>
     *   <li><em>changeReason</em> => a string representing the reason for the changeset</li>
     *   <li><em>after</em> => an array of triples representing the required state of the resource description after the changeset would be applied. All subjects must be the same.</li>
     *   <li><em>before</em> => an array of triples representing the state of the resource description before the changeset is applied. All subjects must be the same.</li>
     *   <li><em>after_rdfxml</em> => a string of RDF/XML representing the required state of the resource description after the changeset would be applied. This is parsed and used to overwrite the 'after' parameter, if any. All subjects must be the same.</li>
     *   <li><em>before_rdfxml</em> => a string of RDF/XML representing the state of the resource description before the changeset is applied. This is parsed and used to overwrite the 'begin' parameter, if any. All subjects must be the same.</li>
     * </ul>
     * If none of 'after', 'before', 'after_rdfxml' or 'before_rdfxml' is supplied then an empty changeset is constructed. <br />
     * The 'after' and 'before' arrays are simple arrays where each element is a triple array with the following structure:
     * <ul>
     *   <li><em>s</em> => the subject URI</li>
     *   <li><em>p</em> => the predicate URI</li>
     *   <li><em>o</em> => the value of the object</li>
     *   <li><em>o_type</em> => one of 'uri', 'bnode' or 'literal'</li>
     *   <li><em>o_lang</em> => the language of the literal if any</li>
     *   <li><em>o_datatype</em> => the data type URI of the literal if any</li>
     * </ul>
     * @param array args an associative array of parameters to use when constructing the changeset
     */
    var $a;
    var $subjectIndex = array();
    var $_index = array();

    function __construct($a = '') {
        parent::__construct();
        $this->a = $a;
        /* parse the before and after graphs if necessary*/
        foreach(array('before','after', 'before_rdfxml', 'after_rdfxml') as $rdf){
            if(!empty($a[$rdf]) ){
                if(is_string($a[$rdf]) ){
                    $parser = ARC2::getRDFParser();
                    $parser->parse(false, $a[$rdf]);
                    $a[$rdf] = $parser->getSimpleIndex(0);
                } else if(
                    is_array($a[$rdf]) AND
                    isset($a[$rdf][0]) AND
                    isset($a[$rdf][0]['s'])
                ) { //triples array
                    $ser = ARC2::getTurtleSerializer();
                    $turtle = $ser->getSerializedTriples($a[$rdf]);
                    $parser = ARC2::getTurtleParser();
                    $parser->parse(false, $turtle);
                    $a[$rdf] = $parser->getSimpleIndex(0);

                }
                $nrdf = str_replace('_rdfxml','',$rdf);
                $this->$nrdf = $a[$rdf] ;

            }
        }
        $this->__init();
    }

    function ChangeSet ($a = '') {
        $this->__construct($a);
    }

    function __init() {
        $csIndex = array();
        $CSNS = 'http://purl.org/vocab/changeset/schema#';

        // Get the triples to be added
        if(empty($this->before)){
            $additions = $this->after;
        } else {
            $additions = ExtendedGraph::diff($this->after, $this->before);
        }
        //Get the triples to be removed
        if(empty($this->after)){
            $removals = $this->before;
        } else {
            $removals = ExtendedGraph::diff($this->before, $this->after);
        }
        // $removals = !empty($this->after)? ExtendedGraph::diff($this->before, $this->after) : $this->before;

        //remove etag triples
        foreach(array('removals' => $removals, 'additions'=> $additions) as $name => $graph){
            foreach($graph as $uri => $properties){
                if(isset($properties["http://schemas.talis.com/2005/dir/schema#etag"])){
                    unset(${$name}[$uri]["http://schemas.talis.com/2005/dir/schema#etag"]);
                    if (count(${$name}[$uri]) == 0)
                    {
                        unset(${$name}[$uri]);
                    }
                }
            }
        }

//    print_r(array_keys($additions));
//    print_r(array_keys($removals));
//    print_r(array_merge(array_keys($additions), array_keys($removals)));

        // Get an array of all the subject uris
        $subjectIndex = !empty($this->a['subjectOfChange'])? array($this->a['subjectOfChange']) : array_unique(array_merge(array_keys($additions), array_keys($removals)));
//    print_r($subjectIndex);

        // Get the metadata for all the changesets
        $date  = (!empty($this->a['createdDate']))? $this->a['createdDate'] : date(DATE_ATOM);
        $creator  = (!empty($this->a['creatorName']))? $this->a['creatorName'] : 'Moriarty ChangeSet Builder';
        $reason  = (!empty($this->a['changeReason']))? $this->a['changeReason'] : 'Change using Moriarty ChangeSet Builder';

        $csCount = 0;
        foreach ($subjectIndex as $subjectOfChange) {
            $csID = '_:cs'.$csCount;
            $csIndex[$subjectOfChange] = $csID;
            $this->addT($csID, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', $CSNS.'ChangeSet', 'uri');
            $subjectType = (strpos($subjectOfChange, '_:')===0)? 'bnode' : 'uri';
            $this->addT($csID, $CSNS.'subjectOfChange', $subjectOfChange, $subjectType);
            $this->addT($csID, $CSNS.'createdDate', $date, 'literal');
            $this->addT($csID, $CSNS.'creatorName', $creator, 'literal');
            $this->addT($csID, $CSNS.'changeReason', $reason, 'literal');

            /* add extra user-given properties to each changeset*/
            if(!empty($this->a['properties'])){
                foreach ($this->a['properties'] as $p => $objs) $this->addT($csID, $p, $objs);
            }
            $csCount++;
        }
        /*iterate through the triples to be added,
        reifying them,
        and linking to the Statements from the appropriate changeset
        */
        $reifiedAdditions = ExtendedGraph::reify($additions, 'Add');
        if(!empty($reifiedAdditions)){
            foreach($reifiedAdditions as $nodeID => $props){
                $subject = $props['http://www.w3.org/1999/02/22-rdf-syntax-ns#subject'][0]['value'];
                if(in_array($subject, $subjectIndex)){
                    $csID = $csIndex[$subject];
                    $this->addT($csID, $CSNS.'addition', $nodeID, 'bnode');
                }

                // if dc:source is given in the instantiating arguments, add it to the statement as provenance
                if(isset($this->a['http://purl.org/dc/terms/source'])){
                    $this->addT($nodeID, 'http://purl.org/dc/terms/source', $this->a['http://purl.org/dc/terms/source'], 'uri');
                }
            }
        }


        /*iterate through the triples to be removed,
        reifying them,
        and linking to the Statements from the appropriate changeset
        */


        $reifiedRemovals = ExtendedGraph::reify($removals, 'Remove');
        foreach($reifiedRemovals as $nodeID => $props){
            $subject = $props['http://www.w3.org/1999/02/22-rdf-syntax-ns#subject'][0]['value'];
            if(in_array($subject, $subjectIndex)){
                $csID = $csIndex[$subject];
                $this->addT($csID, $CSNS.'removal', $nodeID, 'bnode');
            }
        }


        // foreach($this->_index as $uri => $props){
        //  if(
        //      !isset($props[$CSNS.'removal'])
        //      AND
        //      !isset($props[$CSNS.'addition'])
        //      ){
        //        unset($this->_index[$uri]);
        //    }
        //
        // }

        $this->_index = ExtendedGraph::merge($this->_index, $reifiedAdditions, $reifiedRemovals);

    }

    /**
     * adds a triple to the internal simpleIndex holding all the changesets and statements
     * @return void
     * @author Keith
     **/
    function addT($s, $p, $o, $o_type='bnode'){
        if(is_array($o) AND isset($o[0]['type'])){
            foreach($o as $obj){
                $this->addT($s, $p, $obj );
            }
        }else {
            $obj = !is_array($o)? array('value' => $o, 'type'=> $o_type) : $o ;
            $this->_index[$s][$p][]=$obj;
        }
    }

    function toRDFXML(){
        $ser = ARC2::getRDFXMLSerializer();
        return $ser->getSerializedIndex($this->_index);
    }

    function to_rdfxml(){
        return $this->toRDFXML();
    }

    function has_changes(){
        foreach($this->_index as $uri => $properties){
            if(
                isset($properties['http://purl.org/vocab/changeset/schema#addition'])
                OR
                isset($properties['http://purl.org/vocab/changeset/schema#removal'])
            ){
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a unique array of the subjects of change in this changeset
     * @return array
     */
    function get_subjects_of_change() {
        $subjects = array();
        /** @noinspection PhpParamsInspection */
        $changes = $this->get_subjects_of_type($this->qname_to_uri("cs:ChangeSet"));
        foreach ($changes as $change)
        {
            $subjects[] = $this->get_first_resource($change,$this->qname_to_uri("cs:subjectOfChange"));
        }
        return array_unique($subjects);
    }

    /**
     * Returns the predicates of change keyed by the subject
     * @return array
     */
    function get_subjects_and_predicates_of_change() {
        $subjectsAndPredicatesOfChange = array();
        /** @noinspection PhpParamsInspection */
        $changes = $this->get_subjects_of_type($this->qname_to_uri("cs:ChangeSet"));
        foreach ($changes as $change)
        {
            $subject = $this->get_first_resource($change,$this->qname_to_uri("cs:subjectOfChange"));

            if(!isset($subjectsAndPredicatesOfChange[$subject]))
            {
                $subjectsAndPredicatesOfChange[$subject] = array();
            }
            // If resource is not either completely new or deleted, specify the predicates affected
            // todo: q. is this right? What if we have mixed changes?? a. This is to hint to the getImpactedComposites that it should consider all the properties have potentially changed.
            // todo: instead the changeset itself should not care about that and the getImpactedComposites should receive the output of MongoTripodUpdates::applyChangeset and use that to make decisions instead of the Changeset
            if(!(empty($this->before) || empty($this->after)))
            {
                foreach($this->get_subjects_where_resource(RDF_SUBJECT, $subject) as $changeNode)
                {
                    foreach($this->get_resource_triple_values($changeNode, RDF_PREDICATE) as $property)
                    {
                        $subjectsAndPredicatesOfChange[$subject][] = $this->_labeller->uri_to_alias($property);
                    }
                }
            }
        }

        // unique predicates
        foreach($subjectsAndPredicatesOfChange as $subject=>$predicates)
        {
            $subjectsAndPredicatesOfChange[$subject] = array_unique($predicates);
        }

        return $subjectsAndPredicatesOfChange;
    }
}
?>