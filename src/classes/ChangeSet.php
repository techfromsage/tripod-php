<?php

declare(strict_types=1);

namespace Tripod;

/**
 * Represents a changeset. Can be used to create a changeset based on the difference between two bounded descriptions. The descriptions must share the same subject URI.
 * Adapted from Moriarty's changeset.
 *
 * @phpstan-import-type ObjectType from ExtendedGraph
 * @phpstan-import-type ObjectValue from ExtendedGraph
 * @phpstan-import-type TripleSubject from ExtendedGraph
 * @phpstan-import-type TriplePredicate from ExtendedGraph
 * @phpstan-import-type TripleObject from ExtendedGraph
 * @phpstan-import-type TripleGraph from ExtendedGraph
 *
 * @see https://code.google.com/p/moriarty/source/browse/trunk/changeset.class.php
 */
class ChangeSet extends ExtendedGraph
{
    /** @var TripleGraph */
    public array $before = [];

    /** @var TripleGraph */
    public array $after = [];

    /**
     * The raw changeset arguments as supplied to the constructor.
     */
    public array $a;

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
     *
     * @param array $a args an associative array of parameters to use when constructing the changeset
     */
    public function __construct(array $a)
    {
        parent::__construct();
        $this->a = $a;
        // parse the before and after graphs if necessary
        foreach (['before', 'after', 'before_rdfxml', 'after_rdfxml'] as $rdf) {
            if (!empty($a[$rdf])) {
                if (is_string($a[$rdf])) {
                    /** @var \ARC2_RDFParser $parser */
                    $parser = \ARC2::getRDFParser();
                    $parser->parse(false, $a[$rdf]);
                    $a[$rdf] = $parser->getSimpleIndex(0);
                } elseif (
                    is_array($a[$rdf]) && isset($a[$rdf][0]) && is_array($a[$rdf][0]) && isset($a[$rdf][0]['s'])
                ) { // triples array
                    /** @var \ARC2_RDFSerializer $ser */
                    $ser = \ARC2::getTurtleSerializer();

                    /** @var string $turtle */
                    $turtle = $ser->getSerializedTriples($a[$rdf]);

                    /** @var \ARC2_RDFParser $parser */
                    $parser = \ARC2::getTurtleParser();
                    $parser->parse(false, $turtle);
                    $a[$rdf] = $parser->getSimpleIndex(0);
                }

                $nrdf = str_replace('_rdfxml', '', $rdf);
                $this->{$nrdf} = $a[$rdf];
            }
        }

        $this->__init();
    }

    protected function __init(): void
    {
        $csIndex = [];
        $CSNS = 'http://purl.org/vocab/changeset/schema#';
        // Get the triples to be added
        $additions = empty($this->before) ? $this->after : ExtendedGraph::diff($this->after, $this->before);

        // Get the triples to be removed
        $removals = empty($this->after) ? $this->before : ExtendedGraph::diff($this->before, $this->after);

        // remove etag triples
        foreach ($removals as $uri => $properties) {
            if (isset($properties['http://schemas.talis.com/2005/dir/schema#etag'])) {
                unset($removals[$uri]['http://schemas.talis.com/2005/dir/schema#etag']);
                if (count($removals[$uri]) === 0) {
                    unset($removals[$uri]);
                }
            }
        }

        foreach ($additions as $uri => $properties) {
            if (isset($properties['http://schemas.talis.com/2005/dir/schema#etag'])) {
                unset($additions[$uri]['http://schemas.talis.com/2005/dir/schema#etag']);
                if (count($additions[$uri]) === 0) {
                    unset($additions[$uri]);
                }
            }
        }

        // Get an array of all the subject uris
        $subjectIndex = empty($this->a['subjectOfChange']) ? array_unique(array_merge(array_keys($additions), array_keys($removals))) : [$this->a['subjectOfChange']];

        // Get the metadata for all the changesets
        $date = (empty($this->a['createdDate'])) ? date(DATE_ATOM) : $this->a['createdDate'];
        $creator = (empty($this->a['creatorName'])) ? 'Moriarty ChangeSet Builder' : $this->a['creatorName'];
        $reason = (empty($this->a['changeReason'])) ? 'Change using Moriarty ChangeSet Builder' : $this->a['changeReason'];

        $csCount = 0;
        foreach ($subjectIndex as $subjectOfChange) {
            $csID = '_:cs' . $csCount;
            $csIndex[$subjectOfChange] = $csID;
            $this->addT($csID, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', $CSNS . 'ChangeSet', 'uri');
            $subjectType = (strpos($subjectOfChange, '_:') === 0) ? 'bnode' : 'uri';
            $this->addT($csID, $CSNS . 'subjectOfChange', $subjectOfChange, $subjectType);
            $this->addT($csID, $CSNS . 'createdDate', $date, 'literal');
            $this->addT($csID, $CSNS . 'creatorName', $creator, 'literal');
            $this->addT($csID, $CSNS . 'changeReason', $reason, 'literal');

            // add extra user-given properties to each changeset
            if (!empty($this->a['properties'])) {
                foreach ($this->a['properties'] as $p => $objs) {
                    $this->addT($csID, $p, $objs);
                }
            }

            $csCount++;
        }

        /*iterate through the triples to be added,
        reifying them,
        and linking to the Statements from the appropriate changeset
        */
        $reifiedAdditions = ExtendedGraph::reify($additions, 'Add');
        foreach ($reifiedAdditions as $nodeID => $props) {
            $subject = $props['http://www.w3.org/1999/02/22-rdf-syntax-ns#subject'][0]['value'];
            if (is_string($subject) && in_array($subject, $subjectIndex)) {
                $csID = $csIndex[$subject];
                $this->addT($csID, $CSNS . 'addition', $nodeID, 'bnode');
            }

            // if dc:source is given in the instantiating arguments, add it to the statement as provenance
            if (isset($this->a['http://purl.org/dc/terms/source'])) {
                $this->addT($nodeID, 'http://purl.org/dc/terms/source', $this->a['http://purl.org/dc/terms/source'], 'uri');
            }
        }

        /*iterate through the triples to be removed,
        reifying them,
        and linking to the Statements from the appropriate changeset
        */

        $reifiedRemovals = ExtendedGraph::reify($removals, 'Remove');
        foreach ($reifiedRemovals as $nodeID => $props) {
            $subject = $props['http://www.w3.org/1999/02/22-rdf-syntax-ns#subject'][0]['value'];
            if (is_string($subject) && in_array($subject, $subjectIndex)) {
                $csID = $csIndex[$subject];
                $this->addT($csID, $CSNS . 'removal', $nodeID, 'bnode');
            }
        }

        $this->_index = ExtendedGraph::merge($this->_index, $reifiedAdditions, $reifiedRemovals);
    }

    /**
     * adds a triple to the internal simpleIndex holding all the changesets and statements.
     *
     * @param TripleSubject                           $s      Subject URI
     * @param TriplePredicate                         $p      Predicate URI
     * @param ObjectValue|TripleObject|TripleObject[] $o      Object URI or literal value, a single triple object, or a list of triple objects
     * @param ObjectType                              $o_type Object type (bnode, uri, literal)
     *
     * @author Keith
     */
    public function addT(string $s, string $p, $o, $o_type = 'bnode'): void
    {
        if (is_array($o) && isset($o[0]['type'])) {
            foreach ($o as $obj) {
                $this->addT($s, $p, $obj);
            }
        } elseif (is_array($o)) {
            // PHPStan cannot discriminate a single triple object from a list of
            // triple objects, so assert what the isset() check above ruled out.
            /** @var TripleObject $tripleObject */
            $tripleObject = $o;
            $this->_index[$s][$p][] = $tripleObject;
        } else {
            $this->_index[$s][$p][] = ['value' => $o, 'type' => $o_type];
        }
    }

    public function toRDFXML(): string
    {
        /** @var \ARC2_RDFSerializer $ser */
        $ser = \ARC2::getRDFXMLSerializer();

        $serialized = $ser->getSerializedIndex($this->_index);

        return is_string($serialized) ? $serialized : '';
    }

    public function to_rdfxml(): string
    {
        return $this->toRDFXML();
    }

    public function has_changes(): bool
    {
        foreach ($this->_index as $properties) {
            if (
                isset($properties['http://purl.org/vocab/changeset/schema#addition']) || isset($properties['http://purl.org/vocab/changeset/schema#removal'])
            ) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns a unique array of the subjects of change in this changeset.
     *
     * @return string[]
     */
    public function get_subjects_of_change(): array
    {
        $subjects = [];
        $changes = $this->get_subjects_of_type((string) $this->qname_to_uri('cs:ChangeSet'));
        foreach ($changes as $change) {
            $subjectOfChange = $this->get_first_resource($change, (string) $this->qname_to_uri('cs:subjectOfChange'));
            if ($subjectOfChange !== null) {
                $subjects[] = $subjectOfChange;
            }
        }

        return array_unique($subjects);
    }
}
