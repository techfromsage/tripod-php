<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Exceptions\Exception;

/**
 * This class is based on SimpleGraph, part of Moriaty: https://code.google.com/p/moriarty/.
 *
 * @phpstan-type ObjectResource string
 * @phpstan-type ObjectLiteral string|int|float|bool
 * @phpstan-type ObjectType 'bnode'|'uri'|'literal'
 * @phpstan-type ObjectValue ObjectResource|ObjectLiteral
 * @phpstan-type TripleSubject string
 * @phpstan-type TriplePredicate string
 * @phpstan-type ResourceTripleObject array{type: 'bnode'|'uri', value: ObjectResource, lang?: string, datatype?: string}
 * @phpstan-type LiteralTripleObject array{type: 'literal', value: ObjectLiteral, lang?: string, datatype?: string}
 * @phpstan-type TripleObject ResourceTripleObject|LiteralTripleObject
 * @phpstan-type TripleGraph array<TripleSubject, array<TriplePredicate, TripleObject[]>>
 *
 * @see https://code.google.com/p/moriarty/source/browse/trunk/labeller.class.php
 */
class ExtendedGraph
{
    public const rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';

    public const rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

    public const rdf_seq = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq';

    /** @var TripleGraph */
    public array $_index = [];

    /** @var string[] */
    public array $_image_properties = [
        'http://xmlns.com/foaf/0.1/depiction',
        'http://xmlns.com/foaf/0.1/img',
    ];

    /** @var string[] */
    public array $_property_order = [
        'http://www.w3.org/2004/02/skos/core#prefLabel',
        RDFS_LABEL,
        'http://purl.org/dc/terms/title',
        DC_TITLE,
        FOAF_NAME,
        'http://www.w3.org/2004/02/skos/core#definition',
        RDFS_COMMENT,
        'http://purl.org/dc/terms/description',
        DC_DESCRIPTION,
        'http://purl.org/vocab/bio/0.1/olb',
        RDF_TYPE,
    ];

    /**
     * @var string[]
     */
    public array $parser_errors = [];

    /**
     * @var Labeller
     */
    public $_labeller;

    /** @var array<string, string> */
    protected array $_ns = [
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
    ];

    /**
     * Up to the application to decide what constitures the label properties for a given app.
     *
     * @var string[]
     */
    private static array $labelProperties;

    /**
     * @param string|TripleGraph $graph
     */
    public function __construct($graph = null)
    {
        $this->_labeller = new Labeller();
        if ($graph) {
            if (is_string($graph)) {
                $this->add_rdf($graph);
            } else {
                $this->_index = $graph;
            }
        }
    }

    /**
     * Map a portion of a URI to a short prefix for use when serialising the graph.
     *
     * @param string $prefix the namespace prefix to associate with the URI
     * @param string $uri    the URI to associate with the prefix
     */
    public function set_namespace_mapping(string $prefix, string $uri): void
    {
        $this->_labeller->set_namespace_mapping($prefix, $uri);
    }

    /**
     * Convert a QName to a URI using registered namespace prefixes.
     *
     * @param string|null $qName the QName to convert
     *
     * @return string|null the URI corresponding to the QName if a suitable prefix exists, null otherwise
     */
    public function qname_to_uri(?string $qName): ?string
    {
        return $this->_labeller->qname_to_uri($qName);
    }

    /**
     * Convert a URI to a QName using registered namespace prefixes.
     *
     * @param string|null $uri the URI to convert
     *
     * @return string|null the QName corresponding to the URI if a suitable prefix exists, null otherwise
     */
    public function uri_to_qname(?string $uri): ?string
    {
        return $this->_labeller->uri_to_qname($uri);
    }

    public function get_prefix(string $ns): string
    {
        return $this->_labeller->get_prefix($ns);
    }

    public function add_labelling_property(string $p): void
    {
        $this->_labeller->add_labelling_property($p);
    }

    /**
     * Constructs an array containing the type of the resource and its value.
     *
     * @param string $resource a URI or blank node identifier (prefixed with _: e.g. _:name)
     *
     * @return array<string, string> an associative array with two keys: 'type' and 'value'. Type is either bnode or uri
     */
    public function make_resource_array(string $resource): array
    {
        $resource_type = strpos($resource, '_:') === 0 ? 'bnode' : 'uri';

        return ['type' => $resource_type, 'value' => $resource];
    }

    /**
     * Adds a triple with a resource object to the graph.
     *
     * @param TripleSubject       $s the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate     $p the predicate URI of the triple
     * @param ObjectResource|null $o the object of the triple, either a URI or a blank node in the format _:name
     *
     * @return bool true if the triple was new, false if it already existed in the graph
     */
    public function add_resource_triple(string $s, string $p, ?string $o): bool
    {
        if ($this->isValidResource($o)) {
            return $this->_add_triple($s, $p, ['type' => strpos($o, '_:') === 0 ? 'bnode' : 'uri', 'value' => $o]);
        }

        return false;
    }

    /**
     * Adds a triple with a literal object to the graph.
     *
     * @param TripleSubject   $s    the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p    the predicate of the triple as a URI
     * @param ObjectLiteral   $o    the object of the triple as a scalar value
     * @param string|null     $lang the language code of the triple's object (optional)
     * @param string|null     $dt   the datatype URI of the triple's object (optional)
     *
     * @return bool true if the triple was new, false if it already existed in the graph
     */
    public function add_literal_triple(string $s, string $p, $o, ?string $lang = null, ?string $dt = null): bool
    {
        if ($this->isValidLiteral($o)) {
            $o_info = ['type' => 'literal', 'value' => $o];
            if ($lang != null) {
                $o_info['lang'] = $lang;
            }

            if ($dt != null) {
                $o_info['datatype'] = $dt;
            }

            return $this->_add_triple($s, $p, $o_info);
        }

        return false;
    }

    /**
     * @deprecated this is deprecated
     */
    public function get_triples(): array
    {
        return \ARC2::getTriplesFromIndex($this->_to_arc_index($this->_index));
    }

    /**
     * Get a copy of the graph's triple index.
     *
     * @see https://www.easyrdf.org/docs/rdf-formats-php
     *
     * @return TripleGraph
     */
    public function get_index(): array
    {
        return $this->_index;
    }

    /**
     * Serialise the graph to RDF/XML.
     *
     * @return string the RDF/XML version of the graph
     */
    public function to_rdfxml(): string
    {
        /** @var \ARC2_RDFSerializer $serializer */
        $serializer = \ARC2::getRDFXMLSerializer(
            [
                'ns' => $this->_labeller->get_ns(),
            ]
        );

        return $serializer->getSerializedIndex($this->_to_arc_index($this->_index));
    }

    /**
     * Serialise the graph to Turtle.
     *
     * @see http://www.dajobe.org/2004/01/turtle/
     *
     * @return string the Turtle version of the graph
     */
    public function to_turtle(): string
    {
        /** @var \ARC2_RDFSerializer $serializer */
        $serializer = \ARC2::getTurtleSerializer(
            [
                'ns' => $this->_labeller->get_ns(),
            ]
        );

        return $serializer->getSerializedIndex($this->_to_arc_index($this->_index));
    }

    /**
     * Serialise the graph to N-Triples.
     *
     * @see http://www.w3.org/TR/rdf-testcases/#ntriples
     *
     * @return string the N-Triples version of the graph
     */
    public function to_ntriples(): string
    {
        /** @var \ARC2_RDFSerializer $serializer */
        $serializer = \ARC2::getComponent('NTriplesSerializer', []);

        return $serializer->getSerializedIndex($this->_to_arc_index($this->_index));
    }

    /**
     * Serialise the graph to JSON.
     *
     * @see https://www.easyrdf.org/docs/rdf-formats-json
     *
     * @return string the JSON version of the graph
     *
     * @throws Exception if the graph cannot be encoded as JSON
     */
    public function to_json(): string
    {
        $json = json_encode($this->_index);
        if ($json === false) {
            throw new Exception('Failed to serialise graph to JSON: ' . json_last_error_msg());
        }

        return $json;
    }

    /**
     * Serialise the graph to HTML.
     *
     * @param TripleSubject|TripleSubject[]|null $s
     *
     * @return string a HTML version of the graph
     */
    public function to_html($s = null, bool $guess_labels = true): string
    {
        $h = '';

        if ($s) {
            if (is_array($s)) {
                $subjects = array_intersect($s, $this->get_subjects());
                if ($subjects === []) {
                    return '';
                }
            } elseif (isset($this->_index[$s])) {
                $subjects = [$s];
            } else {
                return '';
            }
        } else {
            $subjects = $this->get_subjects();
        }

        foreach ($subjects as $subject) {
            if (count($subjects) > 1) {
                $h .= '<h1><a href="' . htmlspecialchars($subject) . '">' . htmlspecialchars($this->get_label($subject)) . '</a></h1>' . "\n";
            }

            $h .= '<table>' . "\n";

            $properties = $this->get_subject_properties($subject, true);
            $priority_properties = array_intersect($properties, $this->_property_order);
            $properties = array_merge($priority_properties, array_diff($properties, $priority_properties));

            foreach ($properties as $p) {
                $h .= '<tr><th valign="top"><a href="' . htmlspecialchars($p) . '">' . htmlspecialchars($this->get_label($p, true)) . '</a></th>';
                $h .= '<td valign="top">';
                $counter = count($this->_index[$subject][$p]);
                for ($i = 0; $i < $counter; $i++) {
                    if ($i > 0) {
                        $h .= '<br />';
                    }

                    $value = (string) $this->_index[$subject][$p][$i]['value'];
                    if ($this->_index[$subject][$p][$i]['type'] === 'literal') {
                        $h .= htmlspecialchars($value);
                    } else {
                        $h .= '<a href="' . htmlspecialchars($value) . '">';
                        if ($guess_labels) {
                            $h .= htmlspecialchars($this->get_label($value));
                        } else {
                            $h .= htmlspecialchars($value);
                        }

                        $h .= '</a>';
                    }
                }

                $h .= '</td>';
                $h .= '</tr>' . "\n";
            }

            $backlinks = [];
            foreach ($this->_index as $rev_subj => $rev_subj_info) {
                foreach ($rev_subj_info as $rev_subj_p => $rev_subj_p_list) {
                    foreach ($rev_subj_p_list as $rev_value) {
                        if (($rev_value['type'] == 'uri' || $rev_value['type'] == 'bnode') && $rev_value['value'] === $subject) {
                            if (!isset($backlinks[$rev_subj_p])) {
                                $backlinks[$rev_subj_p] = [];
                            }

                            $backlinks[$rev_subj_p][] = $rev_subj;
                        }
                    }
                }
            }

            foreach ($backlinks as $backlink_p => $backlink_values) {
                $h .= '<tr><th valign="top"><a href="' . htmlspecialchars($backlink_p) . '">' . htmlspecialchars($this->get_inverse_label($backlink_p, true)) . '</a></th>';
                $h .= '<td valign="top">';
                $counter = count($backlink_values);
                for ($i = 0; $i < $counter; $i++) {
                    if ($i > 0) {
                        $h .= '<br />';
                    }

                    $h .= '<a href="' . htmlspecialchars($backlink_values[$i]) . '">';
                    if ($guess_labels) {
                        $h .= htmlspecialchars($this->get_label($backlink_values[$i]));
                    } else {
                        $h .= htmlspecialchars($backlink_values[$i]);
                    }

                    $h .= '</a>';
                }

                $h .= '</td>';
                $h .= '</tr>' . "\n";
            }

            $h .= '</table>' . "\n";
        }

        return $h;
    }

    /**
     * Fetch the first literal value for a given subject and predicate. If there are multiple possible values then one is selected at random.
     *
     * @param TripleSubject                     $s       the subject to search for
     * @param TriplePredicate|TriplePredicate[] $p       the predicate to search for, or an array of predicates
     * @param ObjectLiteral|null                $default a default value to use if no literal values are found
     *
     * @return ObjectLiteral|null the first literal value found or the supplied default if no values were found
     */
    public function get_first_literal(string $s, $p, $default = null, ?string $preferred_language = null)
    {
        $best_literal = $default;
        if (isset($this->_index[$s])) {
            if (is_array($p)) {
                foreach ($p as $p_uri) {
                    if (isset($this->_index[$s][$p_uri])) {
                        foreach ($this->_index[$s][$p_uri] as $value) {
                            if ($value['type'] == 'literal') {
                                if ($preferred_language == null) {
                                    return $value['value'];
                                }

                                if (isset($value['lang']) && $value['lang'] == $preferred_language) {
                                    return $value['value'];
                                }

                                $best_literal = $value['value'];
                            }
                        }
                    }
                }
            } elseif (isset($this->_index[$s][$p])) {
                foreach ($this->_index[$s][$p] as $value) {
                    if ($value['type'] == 'literal') {
                        if ($preferred_language == null) {
                            return $value['value'];
                        }

                        if (isset($value['lang']) && $value['lang'] == $preferred_language) {
                            return $value['value'];
                        }

                        $best_literal = $value['value'];
                    }
                }
            }
        }

        return $best_literal;
    }

    /**
     * Fetch the first resource value for a given subject and predicate. If there are multiple possible values then one is selected at random.
     *
     * @param TripleSubject   $s       the subject to search for
     * @param TriplePredicate $p       the predicate to search for
     * @param ObjectResource  $default a default value to use if no literal values are found
     *
     * @return ObjectResource|null the first resource value found or the supplied default if no values were found
     */
    public function get_first_resource(string $s, string $p, ?string $default = null): ?string
    {
        if (isset($this->_index[$s][$p])) {
            foreach ($this->_index[$s][$p] as $value) {
                if ($this->is_resource_object($value)) {
                    return $value['value'];
                }
            }
        }

        return $default;
    }

    /**
     * Remove a triple with a resource object from the graph.
     *
     * @param TripleSubject   $s the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p the predicate URI of the triple
     * @param ObjectResource  $o the object of the triple, either a URI or a blank node in the format _:name
     */
    public function remove_resource_triple(string $s, string $p, $o): void
    {
        // Already removed
        if (!isset($this->_index[$s]) || !isset($this->_index[$s][$p])) {
            return;
        }

        for ($i = count($this->_index[$s][$p]) - 1; $i >= 0; $i--) {
            if (($this->_index[$s][$p][$i]['type'] == 'uri' || $this->_index[$s][$p][$i]['type'] == 'bnode') && $this->_index[$s][$p][$i]['value'] == $o) {
                array_splice($this->_index[$s][$p], $i, 1);
            }
        }

        if (count($this->_index[$s][$p]) === 0) {
            unset($this->_index[$s][$p]);
        }

        if (count($this->_index[$s]) === 0) {
            unset($this->_index[$s]);
        }
    }

    /**
     * @param ObjectLiteral $o
     */
    public function remove_literal_triple(string $s, string $p, $o): void
    {
        // Already removed
        if (!isset($this->_index[$s]) || !isset($this->_index[$s][$p])) {
            return;
        }

        for ($i = count($this->_index[$s][$p]) - 1; $i >= 0; $i--) {
            if ($this->_index[$s][$p][$i]['type'] == 'literal' && $this->_index[$s][$p][$i]['value'] == $o) {
                array_splice($this->_index[$s][$p], $i, 1);
            }
        }

        if (count($this->_index[$s][$p]) === 0) {
            unset($this->_index[$s][$p]);
        }

        if (count($this->_index[$s]) === 0) {
            unset($this->_index[$s]);
        }
    }

    /**
     * Remove all triples having the supplied subject.
     *
     * @param TripleSubject $s the subject of the triple, either a URI or a blank node in the format _:name
     */
    public function remove_triples_about(string $s): void
    {
        unset($this->_index[$s]);
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied RDF/XML.
     *
     * @param string $rdfxml the RDF/XML to parse
     * @param string $base   the base URI against which relative URIs in the RDF/XML document will be resolved
     */
    public function from_rdfxml(string $rdfxml, string $base = ''): void
    {
        if ($rdfxml !== '' && $rdfxml !== '0') {
            $this->remove_all_triples();
            $this->add_rdfxml($rdfxml, $base);
        }
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied JSON.
     *
     * @see https://www.easyrdf.org/docs/rdf-formats-json
     *
     * @param string $json the JSON to parse
     */
    public function from_json(string $json): void
    {
        if ($json !== '' && $json !== '0') {
            $this->remove_all_triples();
            $index = json_decode($json, true);
            if (is_array($index)) {
                $this->_index = $index;
            }
        }
    }

    /**
     * Add the triples parsed from the supplied JSON to the graph.
     *
     * @see https://www.easyrdf.org/docs/rdf-formats-json
     *
     * @param string $json the JSON to parse
     */
    public function add_json(string $json): void
    {
        if ($json !== '' && $json !== '0') {
            $json_index = json_decode($json, true);
            if (is_array($json_index)) {
                $this->_index = $this->merge($this->_index, $json_index);
            }
        }
    }

    /**
     * @return string[]
     */
    public function get_parser_errors(): array
    {
        return $this->parser_errors;
    }

    /**
     * Add the triples parsed from the supplied RDF to the graph - let ARC guess the input.
     *
     * @param string $rdf  the RDF to parse
     * @param string $base the base URI against which relative URIs in the RDF document will be resolved
     *
     * @author Keith Alexander
     */
    public function add_rdf(string $rdf, string $base = ''): void
    {
        $trimRdf = trim($rdf);
        if ($trimRdf[0] == '{') { // lazy is-this-json assessment  - might be better to try json_decode - but more costly
            $this->add_json($trimRdf);
            unset($trimRdf);
        } else {
            /** @var \ARC2_RDFParser $parser */
            $parser = \ARC2::getRDFParser();
            $parser->parse($base, $rdf);
            $errors = $parser->getErrors();
            if (!empty($errors)) {
                $this->parser_errors[] = $errors;
            }

            $triples = $parser->getTriples();
            $this->_add_arc2_triple_list($triples);
            unset($parser);
        }
    }

    /**
     * Add the triples parsed from the supplied RDF/XML to the graph.
     *
     * @param string $rdfxml the RDF/XML to parse
     * @param string $base   the base URI against which relative URIs in the RDF/XML document will be resolved
     */
    public function add_rdfxml(string $rdfxml, string $base = ''): void
    {
        if ($rdfxml !== '' && $rdfxml !== '0') {
            /** @var \ARC2_RDFXMLParser $parser */
            $parser = \ARC2::getRDFXMLParser();
            $parser->parse($base, $rdfxml);
            $triples = $parser->getTriples();
            $this->_add_arc2_triple_list($triples);
            unset($parser);
        }
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied Turtle.
     *
     * @see http://www.dajobe.org/2004/01/turtle/
     *
     * @param string $turtle the Turtle to parse
     * @param string $base   the base URI against which relative URIs in the Turtle document will be resolved
     */
    public function from_turtle(string $turtle, string $base = ''): void
    {
        if ($turtle !== '' && $turtle !== '0') {
            $this->remove_all_triples();
            $this->add_turtle($turtle, $base);
        }
    }

    /**
     * Add the triples parsed from the supplied Turtle to the graph.
     *
     * @see http://www.dajobe.org/2004/01/turtle/
     *
     * @param string $turtle the Turtle to parse
     * @param string $base   the base URI against which relative URIs in the Turtle document will be resolved
     */
    public function add_turtle(string $turtle, string $base = ''): void
    {
        if ($turtle !== '' && $turtle !== '0') {
            /** @var \ARC2_TurtleParser $parser */
            $parser = \ARC2::getTurtleParser();
            $parser->parse($base, $turtle);
            $triples = $parser->getTriples();
            $this->_add_arc2_triple_list($triples);
            unset($parser);
        }
    }

    /**
     * Replace the triples in the graph with those parsed from the supplied RDFa.
     *
     * @param string $html the HTML containing RDFa to parse
     * @param string $base the base URI against which relative URIs in the RDFa document will be resolved
     */
    public function from_rdfa(string $html, string $base = ''): void
    {
        if ($html !== '' && $html !== '0') {
            $this->remove_all_triples();
            $this->add_rdfa($html, $base);
        }
    }

    /**
     * Add the triples parsed from the supplied RDFa to the graph.
     *
     * @param string $html the HTML containing RDFa to parse
     * @param string $base the base URI against which relative URIs in the RDFa document will be resolved
     */
    public function add_rdfa(string $html, string $base = ''): void
    {
        if ($html !== '' && $html !== '0') {
            /** @var \ARC2_SemHTMLParser $parser */
            $parser = \ARC2::getSemHTMLParser();
            $parser->parse($base, $html);
            $parser->extractRDF('rdfa');
            $triples = $parser->getTriples();
            $this->_add_arc2_triple_list($triples);
            unset($parser);
        }
    }

    /**
     * Add the triples in the supplied graph to the current graph.
     *
     * @param ExtendedGraph $g the graph to read
     */
    public function add_graph(ExtendedGraph $g): bool
    {
        $triples_were_added = false;
        $index = $g->get_index();
        foreach ($index as $s => $p_list) {
            foreach ($p_list as $p => $o_list) {
                foreach ($o_list as $o_info) {
                    if ($this->_add_triple($s, $p, $o_info)) {
                        $triples_were_added = true;
                    }
                }
            }
        }

        return $triples_were_added;
    }

    /**
     * Tests whether the graph contains the given triple.
     *
     * @param TripleSubject   $s the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p the predicate URI of the triple
     * @param ObjectResource  $o the object of the triple, either a URI or a blank node in the format _:name
     *
     * @return bool true if the triple exists in the graph, false otherwise
     */
    public function has_resource_triple(string $s, string $p, $o): bool
    {
        if (isset($this->_index[$s][$p])) {
            foreach ($this->_index[$s][$p] as $value) {
                if (($value['type'] == 'uri' || $value['type'] == 'bnode') && $value['value'] === $o) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Tests whether the graph contains the given triple.
     *
     * @param TripleSubject   $s    the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p    the predicate URI of the triple
     * @param ObjectLiteral   $o    the object of the triple as a literal value
     * @param string|null     $lang the language of the object
     * @param string|null     $dt   the datatype of the object
     *
     * @return bool true if the triple exists in the graph, false otherwise
     */
    public function has_literal_triple(string $s, string $p, $o, ?string $lang = null, ?string $dt = null): bool
    {
        if (isset($this->_index[$s][$p])) {
            foreach ($this->_index[$s][$p] as $value) {
                if (($value['type'] == 'literal') && $value['value'] === $o) {
                    if ($lang !== null) {
                        return isset($value['lang']) && $value['lang'] === $lang;
                    }

                    if ($dt !== null) {
                        return isset($value['datatype']) && $value['datatype'] === $dt;
                    }

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Fetch the resource values for a given subject and predicate.
     *
     * @param TripleSubject   $s the subject to search for
     * @param TriplePredicate $p the predicate to search for
     *
     * @return ObjectResource[] list of URIs and blank nodes that are the objects of triples with the supplied subject and predicate
     */
    public function get_resource_triple_values(string $s, string $p): array
    {
        $values = [];
        if (isset($this->_index[$s][$p])) {
            foreach ($this->_index[$s][$p] as $value) {
                if ($this->is_resource_object($value)) {
                    $values[] = $value['value'];
                }
            }
        }

        return $values;
    }

    /**
     * Fetch the literal values for a given subject and predicate.
     *
     * @param TripleSubject                     $s the subject to search for
     * @param TriplePredicate|TriplePredicate[] $p the predicate to search for or an array of predicates
     *
     * @return ObjectLiteral[] list of literals that are the objects of triples with the supplied subject and predicate
     */
    public function get_literal_triple_values(string $s, $p): array
    {
        $values = [];
        if (isset($this->_index[$s])) {
            if (is_array($p)) {
                foreach ($p as $p_uri) {
                    if (isset($this->_index[$s][$p_uri])) {
                        foreach ($this->_index[$s][$p_uri] as $value) {
                            if ($value['type'] == 'literal') {
                                $values[] = $value['value'];
                            }
                        }
                    }
                }
            } elseif (isset($this->_index[$s][$p])) {
                foreach ($this->_index[$s][$p] as $value) {
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
     *
     * @param TripleSubject                     $s the subject to search for
     * @param TriplePredicate|TriplePredicate[] $p the predicate to search for or an array of predicates
     *
     * @return TripleObject[] list of values of triples with the supplied subject and predicate
     */
    public function get_subject_property_values(string $s, $p): array
    {
        $values = [];
        if (!is_array($p)) {
            $p = [$p];
        }

        if (isset($this->_index[$s])) {
            foreach ($p as $pinst) {
                if (isset($this->_index[$s][$pinst])) {
                    foreach ($this->_index[$s][$pinst] as $value) {
                        $values[] = $value;
                    }
                }
            }
        }

        return $values;
    }

    /**
     * Fetch a subgraph where all triples have given subject.
     *
     * @param TripleSubject $s the subject to search for
     *
     * @return ExtendedGraph triples with the supplied subject
     */
    public function get_subject_subgraph(string $s): ExtendedGraph
    {
        $sub = new ExtendedGraph();
        if (isset($this->_index[$s])) {
            $sub->_index[$s] = $this->_index[$s];
        }

        return $sub;
    }

    /**
     * Fetch an array of all the subjects.
     *
     * @return TripleSubject[] list of all the subjects in the graph
     */
    public function get_subjects(): array
    {
        return array_keys($this->_index);
    }

    /**
     * Fetch an array of all the subject that have and rdf type that matches that given.
     *
     * @param ObjectResource $o the type to match
     *
     * @return TripleSubject[] list of all the subjects in the graph that have the given type
     */
    public function get_subjects_of_type(string $o): array
    {
        return $this->get_subjects_where_resource(self::rdf_type, $o);
    }

    /**
     * Fetch an array of all the subjects where the predicate and object match a ?s $p $o triple in the graph and the object is a resource.
     *
     * @param TriplePredicate $p the predicate to match
     * @param ObjectResource  $o the resource object to match
     *
     * @return TripleSubject[] list of all the subjects in the graph that have a triple with the given predicate and resource object
     */
    public function get_subjects_where_resource(string $p, string $o): array
    {
        return array_merge($this->get_subjects_where($p, $o, 'uri'), $this->get_subjects_where($p, $o, 'bnode'));
    }

    /**
     * Fetch an array of all the subjects where the predicate and object match a ?s $p $o triple in the graph and the object is a literal value.
     *
     * @param TriplePredicate $p the predicate to match
     * @param ObjectLiteral   $o the literal object to match
     *
     * @return TripleSubject[] list of all the subjects in the graph that have a triple with the given predicate and literal object
     */
    public function get_subjects_where_literal(string $p, $o): array
    {
        return $this->get_subjects_where($p, $o, 'literal');
    }

    /**
     * Fetch the properties of a given subject and predicate.
     *
     * @param TripleSubject $s        the subject to search for
     * @param bool          $distinct if true then duplicate properties are included only once (optional, default is true)
     *
     * @return TriplePredicate[] list of property URIs
     */
    public function get_subject_properties(string $s, bool $distinct = true): array
    {
        $values = [];
        if (isset($this->_index[$s])) {
            foreach ($this->_index[$s] as $prop => $prop_values) {
                if ($distinct) {
                    $values[] = $prop;
                } else {
                    $counter = count($prop_values);
                    for ($i = 0; $i < $counter; $i++) {
                        $values[] = $prop;
                    }
                }
            }
        }

        return $values;
    }

    /**
     * Tests whether the graph contains a triple with the given subject and predicate.
     *
     * @param TripleSubject   $s the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p the predicate URI of the triple
     *
     * @return bool true if a matching triple exists in the graph, false otherwise
     */
    public function subject_has_property(string $s, string $p): bool
    {
        return isset($this->_index[$s][$p]);
    }

    /**
     * Tests whether the graph contains a triple with the given subject.
     *
     * @param TripleSubject $s the subject of the triple, either a URI or a blank node in the format _:name
     *
     * @return bool true if the graph contains any triples with the specified subject, false otherwise
     */
    public function has_triples_about(string $s): bool
    {
        return isset($this->_index[$s]);
    }

    /**
     * Removes all triples with the given subject and predicate.
     *
     * @param TripleSubject   $s the subject of the triple, either a URI or a blank node in the format _:name
     * @param TriplePredicate $p the predicate URI of the triple
     */
    public function remove_property_values(string $s, string $p): void
    {
        unset($this->_index[$s][$p]);
    }

    /**
     * Clears all triples out of the graph.
     */
    public function remove_all_triples(): void
    {
        $this->_index = [];
    }

    /**
     * Tests whether the graph contains any triples.
     *
     * @return bool true if the graph contains no triples, false otherwise
     */
    public function is_empty(): bool
    {
        return count($this->_index) === 0;
    }

    public function get_label(string $resource_uri, bool $capitalize = false, bool $use_qnames = false): string
    {
        return $this->_labeller->get_label($resource_uri, $this, $capitalize, $use_qnames);
    }

    public function get_inverse_label(string $resource_uri, bool $capitalize = false, bool $use_qnames = false): string
    {
        return $this->_labeller->get_inverse_label($resource_uri, $this, $capitalize, $use_qnames);
    }

    /**
     * @param TripleGraph $resources
     */
    public function reify(array $resources, string $nodeID_prefix = 'Statement'): array
    {
        $RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
        $reified = [];
        $statement_no = 1;
        foreach ($resources as $uri => $properties) {
            foreach ($properties as $property => $objects) {
                foreach ($objects as $object) {
                    while (!isset($statement_nodeID) || isset($resources[$statement_nodeID]) || isset($reified[$statement_nodeID])) {
                        $statement_nodeID = '_:' . $nodeID_prefix . ($statement_no++);
                    }

                    $reified[$statement_nodeID] = [
                        $RDF . 'type' => [
                            ['type' => 'uri', 'value' => $RDF . 'Statement'],
                        ],
                        $RDF . 'subject' => [['type' => (substr($uri, 0, 2) == '_:') ? 'bnode' : 'uri', 'value' => $uri]],
                        $RDF . 'predicate' => [['type' => 'uri', 'value' => $property]],
                        $RDF . 'object' => [$object],
                    ];
                }
            }
        }

        return $reified;
    }

    /**
     * returns a simpleIndex consisting of all the statements from the first array that weren't found in any of the subsequent arrays.
     *
     * @param TripleGraph ...$indices If only one array is passed then the diff is taken against the graph's own index, otherwise the diff is taken against the first array passed as a parameter
     *
     * @author Keith
     */
    public function diff(array ...$indices): array
    {
        if (count($indices) === 1) {
            array_unshift($indices, $this->_index);
        }

        $base = array_shift($indices);
        if (count($base) === 0) {
            return [];
        }

        $diff = [];

        foreach ($base as $base_uri => $base_ps) {
            foreach ($indices as $index) {
                if (!isset($index[$base_uri])) {
                    $diff[$base_uri] = $base_ps;
                } else {
                    foreach ($base_ps as $base_p => $base_obs) {
                        if (!isset($index[$base_uri][$base_p])) {
                            $diff[$base_uri][$base_p] = $base_obs;
                        } else {
                            foreach ($base_obs as $base_o) {
                                // because we want to enforce strict type check
                                // on in_array, we need to ensure that array keys
                                // are ordered the same
                                ksort($base_o);
                                $base_p_values = $index[$base_uri][$base_p];
                                foreach ($base_p_values as &$v) {
                                    ksort($v);
                                }

                                if (!in_array($base_o, $base_p_values, true)) {
                                    $diff[$base_uri][$base_p][] = $base_o;
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
     * merges all  rdf/json-style arrays passed as parameters.
     *
     * @param TripleGraph ...$indices If only one array is passed then the merge is done against the graph's own index, otherwise the merge is done against the first array passed as a parameter
     *
     * @author Keith
     */
    public function merge(array ...$indices): array
    {
        $old_bnodeids = [];
        if (count($indices) === 1) {
            array_unshift($indices, $this->_index);
        }

        $current = array_shift($indices);
        foreach ($indices as $newGraph) {
            foreach ($newGraph as $uri => $properties) {
                /* Make sure that bnode ids don't overlap:
                _:a in g1 isn't the same as _:a in g2 */

                if (substr($uri, 0, 2) == '_:') { // bnode
                    $old_id = $uri;
                    $count = 1;

                    while (
                        isset($current[$uri]) || $old_id != $uri && isset($newGraph[$uri]) || isset($old_bnodeids[$uri])
                    ) {
                        $uri .= $count++;
                    }

                    if ($old_id != $uri) {
                        $old_bnodeids[$old_id] = $uri;
                    }
                }

                if (!empty($properties)) {
                    foreach ($properties as $property => $objects) {
                        foreach ($objects as $object) {
                            // make sure that the new bnode is being used
                            if ($object['type'] == 'bnode' && is_string($object['value'])) {
                                $bnode = $object['value'];

                                if (isset($old_bnodeids[$bnode])) {
                                    $object['value'] = $old_bnodeids[$bnode];
                                } else { // bnode hasn't been transposed
                                    $old_bnode_id = $bnode;
                                    $count = 1;
                                    while (
                                        isset($current[$bnode]) || $object['value'] != $bnode && isset($newGraph[$bnode]) || isset($old_bnodeids[$uri])
                                    ) {
                                        $bnode .= $count++;
                                    }

                                    if ($old_bnode_id != $bnode) {
                                        $old_bnodeids[$old_bnode_id] = $bnode;
                                    }

                                    $object['value'] = $bnode;
                                }
                            }

                            if (!isset($current[$uri][$property]) || !in_array($object, $current[$uri][$property])) {
                                $current[$uri][$property][] = $object;
                            }
                        }
                    }
                }
            }
        }

        return $current;
    }

    public function replace_resource(string $look_for, string $replace_with): void
    {
        $remove_list_resources = [];
        $remove_list_literals = [];
        $add_list_resources = [];
        $add_list_literals = [];
        foreach ($this->_index as $s => $p_list) {
            if ($s == $look_for) {
                foreach ($p_list as $p => $o_list) {
                    if ($p == $look_for) {
                        foreach ($o_list as $o_info) {
                            if (!$this->is_resource_object($o_info)) {
                                $lang = $o_info['lang'] ?? null;
                                $dt = $o_info['datatype'] ?? null;
                                $remove_list_literals[] = [$look_for, $look_for, $o_info['value']];
                                $add_list_literals[] = [$replace_with, $replace_with, $o_info['value'], $lang, $dt];
                            } elseif ($o_info['value'] == $look_for) {
                                $remove_list_resources[] = [$look_for, $look_for, $look_for];
                                $add_list_resources[] = [$replace_with, $replace_with, $replace_with];
                            } else {
                                $remove_list_resources[] = [$look_for, $look_for, $o_info['value']];
                                $add_list_resources[] = [$replace_with, $replace_with, $o_info['value']];
                            }
                        }
                    } else {
                        foreach ($o_list as $o_info) {
                            if (!$this->is_resource_object($o_info)) {
                                $lang = $o_info['lang'] ?? null;
                                $dt = $o_info['datatype'] ?? null;
                                $remove_list_literals[] = [$look_for, $p, $o_info['value']];
                                $add_list_literals[] = [$replace_with, $p, $o_info['value'], $lang, $dt];
                            } elseif ($o_info['value'] == $look_for) {
                                $remove_list_resources[] = [$look_for, $p, $look_for];
                                $add_list_resources[] = [$replace_with, $p, $replace_with];
                            } else {
                                $remove_list_resources[] = [$look_for, $p, $o_info['value']];
                                $add_list_resources[] = [$replace_with, $p, $o_info['value']];
                            }
                        }
                    }
                }
            } else {
                foreach ($p_list as $p => $o_list) {
                    if ($p == $look_for) {
                        foreach ($o_list as $o_info) {
                            if (!$this->is_resource_object($o_info)) {
                                $lang = $o_info['lang'] ?? null;
                                $dt = $o_info['datatype'] ?? null;
                                $remove_list_literals[] = [$s, $look_for, $o_info['value']];
                                $add_list_literals[] = [$s, $replace_with, $o_info['value'], $lang, $dt];
                            } elseif ($o_info['value'] == $look_for) {
                                $remove_list_resources[] = [$s, $look_for, $look_for];
                                $add_list_resources[] = [$s, $replace_with, $replace_with];
                            } else {
                                $remove_list_resources[] = [$s, $look_for, $o_info['value']];
                                $add_list_resources[] = [$s, $replace_with, $o_info['value']];
                            }
                        }
                    } else {
                        foreach ($o_list as $o_info) {
                            if ($o_info['type'] != 'literal' && $o_info['value'] == $look_for) {
                                $remove_list_resources[] = [$s, $p, $look_for];
                                $add_list_resources[] = [$s, $p, $replace_with];
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
            $this->remove_literal_triple($t[0], $t[1], $t[2]);
        }

        foreach ($add_list_literals as $t) {
            $this->add_literal_triple($t[0], $t[1], $t[2], $t[3], $t[4]);
        }
    }

    public function get_list_values(string $listUri): array
    {
        $array = [];
        while (!empty($listUri) && $listUri !== RDF_NIL) {
            $array[] = $this->get_first_resource($listUri, RDF_FIRST);
            $listUri = $this->get_first_resource($listUri, RDF_REST);
        }

        return $array;
    }

    /**
     * @param array<string, mixed> $properties
     */
    public static function initProperties(array $properties): void
    {
        if (array_key_exists('labelProperties', $properties)) {
            self::$labelProperties = $properties['labelProperties'] ?? [];
        }
    }

    /**
     * Replaces $uri1 with $uri2 in subject, predicate and object position.
     */
    public function replace_uris(string $uri1, string $uri2): void
    {
        $index = $this->get_index();
        if (isset($index[$uri1])) {
            $index[$uri2] = $index[$uri1];
            unset($index[$uri1]);
        }

        foreach ($index as $uri => $properties) {
            foreach ($properties as $property => $objects) {
                if ($property == $uri1) {
                    $index[$uri][$uri2] = $objects;
                    $property = $uri2;
                    unset($index[$uri][$uri1]);
                }

                foreach ($objects as $i => $object) {
                    if ($this->is_resource_object($object) && $object['value'] == $uri1) {
                        $object['value'] = $uri2;
                        $index[$uri][$property][$i] = $object;
                    }
                }
            }
        }

        $this->_index = $index;
    }

    /**
     * @param TripleSubject|null   $s
     * @param TriplePredicate|null $p
     * @param ObjectValue|null     $o
     */
    public function get_triple_count(?string $s = null, ?string $p = null, $o = null): int
    {
        $index = $this->get_index();

        if ($index === []) {
            return 0;
        }

        $counter = 0;

        foreach ($index as $uri => $properties) {
            if (($s && ($s == $uri)) || !$s) {
                foreach ($properties as $property => $objects) {
                    if (($p && ($p == $property)) || !$p) {
                        foreach ($objects as $object) {
                            if (($o && $o == $object['value']) || !$o) {
                                $counter++;
                            }
                        }
                    }
                }
            }
        }

        return $counter;
    }

    /**
     * Fetch all the resource values for all subjects.
     *
     * @return ObjectResource[] the resource values found
     */
    public function get_resources(): array
    {
        $resources = [];
        $subjects = $this->get_subjects();
        foreach ($subjects as $subject) {
            $resources[] = $subject;
            $resources = array_merge($resources, $this->get_resources_for_subject($subject));
        }

        return array_unique($resources);
    }

    /**
     * Fetch all the resource values for a given subject.
     *
     * @param TripleSubject $s the subject to search for
     *
     * @return ObjectResource[] the resource values found
     */
    public function get_resources_for_subject(string $s): array
    {
        $resources = [];
        if (isset($this->_index[$s])) {
            foreach ($this->_index[$s] as $values) {
                foreach ($values as $value) {
                    if ($this->is_resource_object($value)) {
                        $resources[] = $value['value'];
                    }
                }
            }
        }

        return array_unique($resources);
    }

    /**
     * @param TriplePredicate $p
     */
    public function remove_properties(string $p): void
    {
        foreach ($this->get_subjects() as $s) {
            $this->remove_property_values($s, $p);
        }
    }

    /**
     * @param TriplePredicate $p
     *
     * @return ObjectResource[] the resource values found
     */
    public function get_resource_properties(string $p): array
    {
        $resources = [];
        foreach ($this->get_subjects() as $s) {
            $properties = $this->get_resource_triple_values($s, $p);
            $resources = array_merge($resources, $properties);
        }

        return $resources;
    }

    /**
     * @param TriplePredicate $p
     * @param ObjectValue     $o
     *
     * @return TripleSubject[]
     */
    public function get_subjects_with_property_value(string $p, $o): array
    {
        $subjects = [];
        foreach ($this->get_subjects() as $s) {
            if ((is_string($o) && $this->has_resource_triple($s, $p, $o)) || $this->has_literal_triple($s, $p, $o)) {
                $subjects[] = $s;
            }
        }

        return $subjects;
    }

    /**
     * @param TripleSubject $sequenceUri
     *
     * @return ObjectValue[]
     */
    public function get_sequence_values(string $sequenceUri): array
    {
        $triples = $this->get_index();
        $properties = [];

        if (isset($triples[$sequenceUri])) {
            foreach ($triples[$sequenceUri] as $property => $objects) {
                if (strpos($property, self::rdf . '_') !== false) {
                    $key = substr($property, strpos($property, '_') + 1);
                    $value = $this->get_first_resource($sequenceUri, $property);

                    if (empty($value)) {
                        $value = $this->get_first_literal($sequenceUri, $property);
                    }

                    $properties[$key] = $value;
                }
            }

            ksort($properties, SORT_NUMERIC);
        }

        return array_values($properties);
    }

    /**
     * @param TripleSubject $sequenceUri
     */
    public function get_next_sequence(string $sequenceUri): int
    {
        $values = $this->get_sequence_values($sequenceUri);

        return count($values) + 1;
    }

    /**
     * @param TripleSubject $s
     * @param ObjectLiteral $o
     */
    public function add_literal_to_sequence(string $s, $o): void
    {
        $this->add_to_sequence($s, $o, 'literal');
    }

    /**
     * Remove a resource from a specified sequence and reindex the sequence to remove the gap.
     *
     * @param TripleSubject  $sequenceUri
     * @param ObjectResource $resourceValue
     */
    public function remove_resource_from_sequence(string $sequenceUri, string $resourceValue): void
    {
        $sequenceProperties = $this->get_subject_properties($sequenceUri);
        $sequenceValues = $this->get_sequence_values($sequenceUri);

        // Remove existing data
        foreach ($sequenceProperties as $sequenceProperty) {
            if (strpos($sequenceProperty, self::rdf . '_') !== false) {
                $sequencePropertyValue = $this->get_first_resource($sequenceUri, $sequenceProperty);
                $this->remove_resource_triple($sequenceUri, $sequenceProperty, $sequencePropertyValue);
            }
        }

        // Recreate the sequence with the correct indexing.
        foreach ($sequenceValues as $sequenceValue) {
            if ($sequenceValue != $resourceValue) {
                $this->add_resource_to_sequence($sequenceUri, (string) $sequenceValue);
            }
        }
    }

    /**
     * @param TripleSubject  $s
     * @param ObjectResource $o
     */
    public function add_resource_to_sequence(string $s, string $o): void
    {
        $this->add_to_sequence($s, $o, 'resource');
    }

    /**
     * @param TripleSubject  $s
     * @param ObjectResource $o
     */
    public function add_resource_to_sequence_in_position(string $s, string $o, int $position): void
    {
        $sequenceValues = $this->get_sequence_values($s);

        if ($sequenceValues === [] || $position > count($sequenceValues)) {
            $this->add_resource_to_sequence($s, $o);
        } else {
            array_splice($sequenceValues, $position - 1, 1, [$o, $sequenceValues[$position - 1]]);

            $properties = $this->get_subject_properties($s);
            foreach ($properties as $p) {
                if (strpos($p, self::rdf . '_') !== false) {
                    $this->remove_property_values($s, $p);
                }
            }

            foreach ($sequenceValues as $value) {
                $this->add_resource_to_sequence($s, (string) $value);
            }
        }
    }

    /**
     * @param ObjectLiteral $oOldValue
     * @param ObjectLiteral $oNewValue
     */
    public function replace_literal_triple(string $s, string $p, $oOldValue, $oNewValue): bool
    {
        if ($this->has_literal_triple($s, $p, $oOldValue)) {
            $this->remove_literal_triple($s, $p, $oOldValue);
            $this->add_literal_triple($s, $p, $oNewValue);

            return true;
        }

        return false;
    }

    /**
     * @param TripleSubject       $s
     * @param TriplePredicate     $p
     * @param ObjectResource|null $o
     */
    public function replace_resource_triples(string $s, string $p, ?string $o): void
    {
        if ($this->subject_has_property($s, $p)) {
            $this->remove_property_values($s, $p);
        }

        if (!empty($o)) {
            $this->add_resource_triple($s, $p, $o);
        }
    }

    /**
     * @param TripleSubject      $s
     * @param TriplePredicate    $p
     * @param ObjectLiteral|null $o
     */
    public function replace_literal_triples(string $s, string $p, $o): void
    {
        if ($this->subject_has_property($s, $p)) {
            $this->remove_property_values($s, $p);
        }

        if (!empty($o)) {
            $this->add_literal_triple($s, $p, $o);
        }
    }

    /**
     * @param TripleSubject $uri
     *
     * @throws Exception
     */
    public function get_label_for_uri(string $uri): string
    {
        if (empty($this->_index[$uri])) {
            return '';
        }

        if (empty(self::$labelProperties)) {
            throw new Exception('Please initialise ExtendedGraph::$labelProperties');
        }

        foreach (self::$labelProperties as $p) {
            if (isset($this->_index[$uri][$p])) {
                return (string) $this->_index[$uri][$p][0]['value'];
            }
        }

        return '';
    }

    public function is_equal_to(ExtendedGraph $otherGraph): bool
    {
        $diffThisAndThat = $this->diff($this->get_index(), $otherGraph->get_index());
        $diffThatAndThis = $this->diff($otherGraph->get_index(), $this->get_index());

        return $diffThisAndThat === [] && $diffThatAndThis === [];
    }

    /**
     * @param ObjectResource $type
     */
    public function remove_subjects_of_type(string $type): void
    {
        $subjects = $this->get_subjects_of_type($type);
        foreach ($subjects as $s) {
            $this->remove_triples_about($s);
        }
    }

    public function from_graph(ExtendedGraph $graph): void
    {
        $this->remove_all_triples();
        $this->add_graph($graph);
    }

    /**
     * Whether a triple object represents a resource (URI or blank node) rather than a literal.
     * Resource objects always carry a string value.
     *
     * @param TripleObject $o
     *
     * @phpstan-assert-if-true ResourceTripleObject $o
     */
    protected function is_resource_object(array $o): bool
    {
        return $o['type'] == 'uri' || $o['type'] == 'bnode';
    }

    /**
     * Check if a triple value is valid.
     *
     * Ideally a valid literal value should be a string
     * but accepting scalars so we can handle legacy data
     * which was not type-checked.
     *
     * @param mixed $value
     */
    protected function isValidLiteral($value): bool
    {
        return is_scalar($value);
    }

    /**
     * Check if a triple value is valid.
     *
     * @param mixed $value
     */
    protected function isValidResource($value): bool
    {
        return is_string($value) && ($value !== '' && $value !== '0');
    }

    /**
     * @param TripleSubject   $s
     * @param TriplePredicate $p
     * @param TripleObject    $o_info
     *
     * @throws Exception
     */
    private function _add_triple(string $s, string $p, array $o_info): bool
    {
        // The value $o should already have been validated by this point
        // It's validation differs depending on whether it is a literal or resource
        // So just check the subject and predicate here...
        if (!$this->isValidResource($s)) {
            throw new Exception('The subject is invalid');
        }

        if (!$this->isValidResource($p)) {
            throw new Exception('The predicate is invalid');
        }

        if (!isset($this->_index[$s])) {
            $this->_index[$s] = [];
            $this->_index[$s][$p] = [$o_info];

            return true;
        }

        if (!isset($this->_index[$s][$p])) {
            $this->_index[$s][$p] = [$o_info];

            return true;
        }

        if (!in_array($o_info, $this->_index[$s][$p])) {
            $this->_index[$s][$p][] = $o_info;

            return true;
        }

        return false;
    }

    private function _add_arc2_triple_list(array &$triples): void
    {
        $bnode_index = [];

        // We can safely preserve bnode labels if the graph is empty, otherwise we need to rewrite them
        $rewrite_bnode_labels = !$this->is_empty();

        foreach ($triples as $t) {
            $obj = [];

            if ($rewrite_bnode_labels && $t['o_type'] == 'bnode') {
                if (!array_key_exists($t['o'], $bnode_index)) {
                    $bnode_index[$t['o']] = uniqid('_:mor');
                }

                $obj['value'] = $bnode_index[$t['o']];
            } else {
                $obj['value'] = $t['o'];
            }

            if ($rewrite_bnode_labels && strpos($t['s'], '_:') === 0) {
                if (!array_key_exists($t['s'], $bnode_index)) {
                    $bnode_index[$t['s']] = uniqid('_:mor');
                }

                $t['s'] = $bnode_index[$t['s']];
            }

            if ($t['o_type'] === 'iri') {
                $obj['type'] = 'uri';
            } elseif (in_array($t['o_type'], ['literal1', 'literal2', 'long_literal1', 'long_literal2'], true)) {
                $obj['type'] = 'literal';
            } else {
                $obj['type'] = $t['o_type'];
            }

            if ($obj['type'] == 'literal') {
                if (isset($t['o_dt']) && $t['o_dt']) {
                    $obj['datatype'] = $t['o_dt'];
                } elseif (isset($t['o_datatype']) && $t['o_datatype']) {
                    $obj['datatype'] = $t['o_datatype'];
                }

                if (isset($t['o_lang']) && $t['o_lang']) {
                    $obj['lang'] = $t['o_lang'];
                }
            }

            if (!isset($this->_index[$t['s']])) {
                $this->_index[$t['s']] = [];
                $this->_index[$t['s']][$t['p']] = [$obj];
            } elseif (!isset($this->_index[$t['s']][$t['p']])) {
                $this->_index[$t['s']][$t['p']] = [$obj];
            } elseif (!in_array($obj, $this->_index[$t['s']][$t['p']])) {
                $this->_index[$t['s']][$t['p']][] = $obj;
            }
        }
    }

    /**
     * @param TripleGraph $index
     */
    private function _to_arc_index(array $index): array
    {
        $ret = [];

        foreach ($index as $s => $s_info) {
            $ret[$s] = [];
            foreach ($s_info as $p => $p_info) {
                $ret[$s][$p] = [];
                foreach ($p_info as $o) {
                    $o_new = [];
                    foreach ($o as $key => $value) {
                        // until ARC2 upgrades to support RDF/PHP we
                        // need to rename all types of "uri" to "iri"
                        if ($key == 'type' && $value == 'uri') {
                            $o_new['type'] = 'iri';
                        } else {
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
     * @param TriplePredicate $p
     * @param ObjectValue     $o
     * @param ObjectType      $type
     *
     * @return TripleSubject[] list of all the subjects in the graph that have a triple with the given predicate, object and object type
     */
    private function get_subjects_where(string $p, $o, string $type): array
    {
        $subjects = [];
        foreach ($this->_index as $subject => $properties) {
            if (isset($properties[$p])) {
                foreach ($properties[$p] as $object) {
                    if ($object['type'] == $type && $object['value'] == $o) {
                        $subjects[] = $subject;

                        break;
                    }
                }
            }
        }

        return $subjects;
    }

    /**
     * @param TripleSubject        $s
     * @param ObjectValue          $o
     * @param 'literal'|'resource' $type
     */
    private function add_to_sequence(string $s, $o, string $type = 'resource'): void
    {
        $sequenceValue = $this->get_next_sequence($s);
        $this->add_resource_triple($s, self::rdf_type, self::rdf_seq);

        if ($type === 'literal') {
            $this->add_literal_triple($s, self::rdf . ('_' . $sequenceValue), $o);
        } else {
            $this->add_resource_triple($s, self::rdf . ('_' . $sequenceValue), (string) $o);
        }
    }
}
