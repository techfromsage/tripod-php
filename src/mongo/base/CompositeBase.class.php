<?php

namespace Tripod\Mongo;

/**
 * Class CompositeBase
 * @package Tripod\Mongo
 */
abstract class CompositeBase extends TripodBase implements IComposite
{
    /**
     * Returns an array of ImpactedSubjects based on the subjects and predicates of change
     * @param array $subjectsAndPredicatesOfChange
     * @param string $contextAlias
     * @return ImpactedSubject[]
     */
    public function getImpactedSubjects(Array $subjectsAndPredicatesOfChange,$contextAlias)
    {
        $candidates = array();
        $filter = array();
        $subjectsToAlias = array();
        foreach(array_keys($subjectsAndPredicatesOfChange) as $s){
            $resourceAlias = $this->labeller->uri_to_alias($s);
            $subjectsToAlias[$s] = $resourceAlias;
            // build $filter for queries to impact index
            $filter[] = array(_ID_RESOURCE=>$resourceAlias,_ID_CONTEXT=>$contextAlias);
        }
        $query = array(_ID_KEY=>array('$in'=>$filter));
        $docs = $this->getCollection()->find($query, array(_ID_KEY=>true, 'rdf:type'=>true));

        $types = $this->getTypesInSpecification();

        if($docs->count() !== 0 ) {
            foreach($docs as $doc)
            {
                $docResource = $doc[_ID_KEY][_ID_RESOURCE];
                $docContext  = $doc[_ID_KEY][_ID_CONTEXT];
                $docHash     = md5($docResource.$docContext);

                $docTypes = array();
                if(isset($doc["rdf:type"])) {
                    if(isset($doc["rdf:type"][VALUE_URI])){
                        $docTypes[] = $doc["rdf:type"][VALUE_URI];
                    } else {
                        foreach($doc["rdf:type"] as $t){
                            if(isset($t[VALUE_URI]))
                            {
                                $docTypes[] = $t[VALUE_URI];
                            }
                        }
                    }
                }

                $currentSubjectProperties = array();
                if(isset($subjectsAndPredicatesOfChange[$docResource]))
                {
                    $currentSubjectProperties = $subjectsAndPredicatesOfChange[$docResource];
                }
                elseif(isset($subjectsToAlias[$docResource]) &&
                    isset($subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]]))
                {
                    $currentSubjectProperties = $subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]];
                }
                foreach($docTypes as $type)
                {
                    if($this->checkIfTypeShouldTriggerOperation($type, $types, $currentSubjectProperties)) {
                        if(!array_key_exists($this->getPodName(), $candidates))
                        {
                            $candidates[$this->getPodName()] = array();
                        }
                        if(!array_key_exists($docHash, $candidates[$this->getPodName()])){
                            $candidates[$this->getPodName()][$docHash] = array('id'=>$doc[_ID_KEY]);
                        }
                    }
                }
            }
        }

        // add to this any composites
        foreach($this->findImpactedComposites($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
            $spec = $this->getSpecification($this->storeName, $doc[_ID_KEY]['type']);
            if(is_array($spec) && array_key_exists('from', $spec)){
                if(!array_key_exists($spec['from'], $candidates))
                {
                    $candidates[$spec['from']] = array();
                }
                $docHash = md5($doc[_ID_KEY][_ID_RESOURCE] . $doc[_ID_KEY][_ID_CONTEXT]);

                if(!array_key_exists($docHash, $candidates[$spec['from']])){
                    $candidates[$spec['from']][$docHash] = array(
                        'id'=>array(
                            _ID_RESOURCE=>$doc[_ID_KEY][_ID_RESOURCE],
                            _ID_CONTEXT=>$doc[_ID_KEY][_ID_CONTEXT],
                        )
                    );
                }
                if(!array_key_exists('specTypes', $candidates[$spec['from']][$docHash])) {
                    $candidates[$spec['from']][$docHash]['specTypes'] = array();
                }
                // Save the specification type so we only have to regen resources in that table type
                if(!in_array($doc[_ID_KEY][_ID_TYPE], $candidates[$spec['from']][$docHash]['specTypes']))
                {
                    $candidates[$spec['from']][$docHash]['specTypes'][] = $doc[_ID_KEY][_ID_TYPE];
                }
            }
        }

        // convert operations to subjects
        $impactedSubjects = array();
        foreach(array_keys($candidates) as $podName){
            foreach($candidates[$podName] as $candidate)
            {
                $specTypes = (isset($candidate['specTypes']) ? $candidate['specTypes'] : array());
                $impactedSubjects[] = new ImpactedSubject($candidate['id'], $this->getOperationType(), $this->getStoreName(), $podName, $specTypes);
            }
        }
        return $impactedSubjects;
    }

    /**
     * Returns an array of the rdf types that will trigger the specification
     * @return array
     */
    public abstract function getTypesInSpecification();

    /**
     * @param array $resourcesAndPredicates
     * @param string $contextAlias
     * @return mixed // @todo: This may eventually return a either a MongoCursor or array
     */
    public abstract function findImpactedComposites(Array $resourcesAndPredicates,$contextAlias);

    /**
     * Returns the specification config
     * @param string $storeName
     * @param string $composite_id The specification id
     * @return array|null
     */
    public abstract function getSpecification($storeName, $composite_id);

    /**
     * Test if the a particular type appears in the array of types associated with a particular spec and that the changeset
     * includes rdf:type (or is empty, meaning addition or deletion vs. update)
     * @param string $rdfType
     * @param array $validTypes
     * @param array $subjectPredicates
     * @return bool
     */
    protected function checkIfTypeShouldTriggerOperation($rdfType, array $validTypes, Array $subjectPredicates)
    {
        // We don't know if this is an alias or a fqURI, nor what is in the valid types, necessarily
        $types = array($rdfType);
        try
        {
            $types[] = $this->labeller->qname_to_uri($rdfType);
        }
        catch(\Tripod\Exceptions\LabellerException $e) {}
        try
        {
            $types[] = $this->labeller->uri_to_alias($rdfType);
        }
        catch(\Tripod\Exceptions\LabellerException $e) {}

        $intersectingTypes = array_unique(array_intersect($types, $validTypes));
        if(!empty($intersectingTypes))
        {
            // Views should always invalidate
            if($this instanceof Views)
            {
                return true;
            }

            // Table rows and Search documents only need to be invalidated if their rdf:type property has changed
            // This means we're either adding or deleting a graph
            if(empty($subjectPredicates))
            {
                return true;
            }
            // Check for alias in changed predicates
            elseif(in_array('rdf:type', $subjectPredicates))
            {
                return true;
            }
            // Check for fully qualified URI in changed predicates
            elseif(in_array(RDF_TYPE, $subjectPredicates))
            {
                return true;
            }
        }
        return false;
    }

}
