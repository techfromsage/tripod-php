<?php
/**
 * Created by JetBrains PhpStorm.
 * User: chris
 * Date: 24/04/2015
 * Time: 15:05
 * To change this template use File | Settings | File Templates.
 */
abstract class CompositeBase extends MongoTripodBase implements IComposite
{
    public function getImpactedSubjects(ChangeSet $cs,$contextAlias)
    {
        $subjectsAndPredicatesOfChange = $cs->get_subjects_and_predicates_of_change();

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

                $currentSubject = null;
                if(isset($subjectsAndPredicatesOfChange[$docResource]))
                {
                    $currentSubject = $subjectsAndPredicatesOfChange[$docResource];
                }
                elseif(isset($subjectsToAlias[$docResource]) &&
                    isset($subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]]))
                {
                    $currentSubject = $subjectsAndPredicatesOfChange[$subjectsToAlias[$docResource]];
                }
                foreach($docTypes as $type)
                {
                    if($this->checkIfTypeShouldTriggerOperation($type, $types, $currentSubject)) {
                        if(!array_key_exists($docHash, $candidates)){
                            $candidates[$docHash] = array('id'=>$doc[_ID_KEY], 'ops'=>array());
                            $candidates[$docHash]['ops'][$this->getPodName()] = array();
                        }
                        array_push($candidates[$docHash]['ops'][$this->getPodName()], $this->getOperationType());
                    }
                }
            }
        }

        // add to this any composites
        foreach($this->findImpactedComposites($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
            $spec = $this->getSpecification($this->storeName, $doc[_ID_KEY]['type']);
            if(!empty($spec)){
                $docHash = md5($doc[_ID_KEY][_ID_RESOURCE] . $doc[_ID_KEY][_ID_CONTEXT]);

                if(!array_key_exists($docHash, $candidates)){
                    $candidates[$docHash] = array(
                        'id'=>array(
                            _ID_RESOURCE=>$doc[_ID_KEY][_ID_RESOURCE],
                            _ID_CONTEXT=>$doc[_ID_KEY][_ID_CONTEXT],
                        )
                    );
                }
                if(!array_key_exists('specTypes', $candidates[$docHash])) {
                    $candidates[$docHash]['specTypes'] = array();
                }
                // Save the specification type so we only have to regen resources in that table type
                if(!in_array($doc[_ID_KEY][_ID_TYPE], $candidates[$docHash]['specTypes']))
                {
                    $candidates[$docHash]['specTypes'][] = $doc[_ID_KEY][_ID_TYPE];
                }
            }
        }

        // convert operations to subjects
        $impactedSubjects = array();
        foreach($candidates as $candidate){
            $specTypes = (isset($candidate['specTypes']) ? $candidate['specTypes'] : array());
            $impactedSubjects[] = new ImpactedSubject($candidate['id'], $this->getOperationType(), $this->getStoreName(), $this->getPodName(), $specTypes);
        }
        return $impactedSubjects;
    }

    public abstract function getTypesInSpecification();

    public abstract function findImpactedComposites($resourcesAndPredicates,$contextAlias);

    public abstract function getSpecification($storeName, $composite_id);

    /**
     * Test if the a particular type appears in the array of types associated with a particular spec and that the changeset
     * includes rdf:type (or is empty, meaning addition or deletion vs. update)
     * @param string $rdfType
     * @param array $validTypes
     * @param array|null $subjectPredicates
     * @return bool
     */
    protected function checkIfTypeShouldTriggerOperation($rdfType, array $validTypes, $subjectPredicates)
    {
        //todo: views wasn't using this code. Figure out why.
        // We don't know if this is an alias or a fqURI, nor what is in the valid types, necessarily
        $types = array($rdfType);
        try
        {
            $types[] = $this->labeller->qname_to_uri($rdfType);
        }
        catch(TripodLabellerException $e) {}
        try
        {
            $types[] = $this->labeller->uri_to_alias($rdfType);
        }
        catch(TripodLabellerException $e) {}

        $intersectingTypes = array_unique(array_intersect($types, $validTypes));
        if(!empty($intersectingTypes))
        {
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
