<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use Tripod\Exceptions\Exception;
use Tripod\Exceptions\LabellerException;
use Tripod\ExtendedGraph;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';

class MongoGraph extends ExtendedGraph
{
    /**
     * @var Labeller
     */
    public $_labeller;

    /**
     * Constructor.
     */
    public function __construct()
    {
        $this->_labeller = new Labeller();
    }

    /**
     * Convert a QName to a URI using registered namespace prefixes. Unlike the base graph
     * this uses the Mongo labeller, which throws for unresolvable qnames instead of
     * returning null, so the result is always a string.
     *
     * @return ($qName is null ? null : string)
     *
     * @throws LabellerException
     */
    public function qname_to_uri(?string $qName): ?string
    {
        return $this->_labeller->qname_to_uri($qName);
    }

    /**
     * Given a context this method serializes the current graph to nquads of the form
     *  <s> <p> <o> <context> .
     *
     * @param string $context the context for the graph your are serializing
     *
     * @return string the nquad serialization of the graph
     *
     * @throws \InvalidArgumentException if you do not specify a context
     */
    public function to_nquads(?string $context): string
    {
        if (empty($context)) {
            throw new \InvalidArgumentException('You must specify the context when serializing to nquads');
        }

        $serializer = new NQuadSerializer();

        return $serializer->getSerializedIndex($this->_index, $this->_labeller->qname_to_alias($context));
    }

    /**
     * Adds the tripod array(s) to this graph.
     * This method is used to add individual tripod documents, or a series of tripod array documents that are embedded in a view.
     */
    public function add_tripod_array(array $tarray): void
    {
        // need to convert from tripod storage format to rdf/json as php array format
        if (isset($tarray['value'][_GRAPHS])) {
            // iterate add add each graph
            foreach ($tarray['value'][_GRAPHS] as $graph) {
                $this->add_tarray_to_index($graph);
            }
        } else {
            $this->add_tarray_to_index($tarray);
        }
    }

    /**
     * Returns a mongo-ready doc for a single CBD.
     */
    public function to_tripod_array(string $docId, ?string $context): ?array
    {
        $docId = $this->_labeller->qname_to_alias($docId);
        $contextAlias = $this->_labeller->uri_to_alias($context);

        if ($docId != null) {
            $subjects = $this->get_subjects();
            foreach ($subjects as $subject) {
                if ($subject == $docId) {
                    $graph = $this->get_subject_subgraph($subject);

                    return $this->index_to_tarray($graph, $contextAlias);
                }
            }
        }

        return null;
    }

    /**
     * Returns a mongo-ready doc for views, which can have multiple graphs in the same doc.
     *
     * @return array<string, mixed>
     */
    public function to_tripod_view_array(string $docId, ?string $context): array
    {
        $subjects = $this->get_subjects();
        $contextAlias = $this->_labeller->uri_to_alias($context);

        // view
        $doc = [];
        $doc[_ID_KEY] = [_ID_RESOURCE => $docId, _ID_CONTEXT => $contextAlias];
        $doc['value'] = [];
        $doc['value'][_IMPACT_INDEX] = $subjects;
        $doc['value'][_GRAPHS] = [];
        foreach ($subjects as $subject) {
            $graph = $this->get_subject_subgraph($subject);
            $doc['value'][_GRAPHS][] = $this->index_to_tarray($graph, $contextAlias);
        }

        return $doc;
    }

    /**
     * @param array<string, mixed> $tarray
     *
     * @throws Exception
     */
    private function add_tarray_to_index(array $tarray): void
    {
        $_i = [];
        $predObjects = [];
        foreach ($tarray as $key => $value) {
            if (!$this->isValidResource($key)) {
                throw new Exception('The predicate must be a non-empty string, ' . var_export($key, true) . ' given');
            }

            if ($key[0] != '_') {
                $predicate = $this->qname_to_uri($key);
                $graphValueObject = $this->toGraphValueObject($value);
                // Only add if valid values have been found
                if ($graphValueObject !== []) {
                    $predObjects[$predicate] = $graphValueObject;
                }
            } elseif ($key == _ID_KEY) {
                // If the subject is invalid then throw an exception
                if (!isset($value[_ID_RESOURCE]) || !$this->isValidResource($value[_ID_RESOURCE])) {
                    throw new Exception('The subject must be a non-empty string, ' . var_export($value[_ID_RESOURCE], true) . ' given');
                }
            }
        }

        $_i[$this->_labeller->qname_to_alias($tarray[_ID_KEY][_ID_RESOURCE])] = $predObjects;
        $this->_index = $this->merge($this->_index, $_i);
    }

    /**
     * Convert from Tripod value object format (comapct) to ExtendedGraph format (verbose).
     *
     * @param array<string, mixed> $mongoValueObject
     */
    private function toGraphValueObject(array $mongoValueObject): array
    {
        $simpleGraphValueObject = [];

        if (array_key_exists(VALUE_LITERAL, $mongoValueObject)) {
            // only allow valid values
            if ($this->isValidLiteral($mongoValueObject[VALUE_LITERAL])) {
                // single value literal
                $simpleGraphValueObject[] = [
                    'type' => 'literal',
                    'value' => $mongoValueObject[VALUE_LITERAL],
                ];
            }
        } elseif (array_key_exists(VALUE_URI, $mongoValueObject)) {
            // only allow valid values
            if ($this->isValidResource($mongoValueObject[VALUE_URI])) {
                // single value uri
                $simpleGraphValueObject[] = [
                    'type' => 'uri',
                    'value' => $this->_labeller->qname_to_alias($mongoValueObject[VALUE_URI]),
                ];
            }
        } else {
            // If we have an array of values
            foreach ($mongoValueObject as $kvp) {
                foreach ($kvp as $type => $value) {
                    // Make sure the value is valid
                    if ($type == VALUE_LITERAL) {
                        if (!$this->isValidLiteral($value)) {
                            continue;
                        }

                        $valueTypeLabel = 'literal';
                    } else {
                        if (!$this->isValidResource($value)) {
                            continue;
                        }

                        $valueTypeLabel = 'uri';
                    }

                    $simpleGraphValueObject[] = [
                        'type' => $valueTypeLabel,
                        'value' => ($type == VALUE_URI) ? $this->_labeller->qname_to_alias($value) : $value,
                    ];
                }
            }
        }

        // Otherwise we have found valid values
        return $simpleGraphValueObject;
    }

    /**
     * Convert from ExtendedGraph value object format (verbose) to Tripod format (compact).
     *
     * @param array<string, mixed> $simpleGraphValueObject
     */
    private function toMongoTripodValueObject(array $simpleGraphValueObject): array
    {
        $valueTypeProp = ($simpleGraphValueObject['type'] == 'literal') ? VALUE_LITERAL : VALUE_URI;

        return [
            $valueTypeProp => ($simpleGraphValueObject['type'] == 'literal') ? $simpleGraphValueObject['value'] : $this->_labeller->uri_to_alias($simpleGraphValueObject['value']),
        ];
    }

    private function index_to_tarray(?ExtendedGraph $graph = null, ?string $contextAlias = null): ?array
    {
        if ($graph == null) {
            $graph = $this;
        }

        $_i = $graph->_index;

        foreach ($_i as $resource => $predObjects) {
            $doc = [];
            $id = [];

            $id[_ID_RESOURCE] = $this->_labeller->uri_to_alias($resource);
            $id[_ID_CONTEXT] = $contextAlias;
            $doc[_ID_KEY] = $id;
            foreach ($predObjects as $predicate => $objects) {
                $pQName = $this->uri_to_qname($predicate);
                if (count($objects) === 1) {
                    $doc[$pQName] = $this->toMongoTripodValueObject($objects[0]);
                } else {
                    $values = [];
                    foreach ($objects as $obj) {
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
