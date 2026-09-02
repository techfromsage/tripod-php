<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Collection;
use Tripod\Config;
use Tripod\Exceptions\LabellerException;

class TriplesUtil
{
    private Labeller $labeller;

    /**
     * Constructor.
     */
    public function __construct()
    {
        $this->labeller = new Labeller();
    }

    /**
     * Add $triples about a given $subject to Mongo. Only $triples with subject matching $subject will be added, others will be ignored.
     * Make them quads with a $context.
     *
     * @param string[]|null $allowableTypes
     */
    public function loadTriplesAbout(string $subject, array $triples, string $storeName, string $podName, ?string $context = null, ?array $allowableTypes = null): void
    {
        $context = $context !== null ? $this->labeller->uri_to_alias($context) : Config::getInstance()->getDefaultContextAlias();
        $collection = Config::getInstance()->getCollectionForCBD($storeName, $podName);

        $graph = new MongoGraph();
        foreach ($triples as $triple) {
            $triple = rtrim($triple);

            $parts = $this->splitTriple($triple);
            $subject = trim($parts[0], '><');
            $predicate = trim($parts[1], '><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object)) {
                $graph->add_resource_triple($subject, $predicate, $object);
            } else {
                $graph->add_literal_triple($subject, $predicate, $object);
            }
        }

        if ($allowableTypes != null) {
            $types = $graph->get_resource_triple_values($subject, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type');

            foreach ($types as $type) {
                if (in_array($type, $allowableTypes)) {
                    $this->saveCBD($subject, $graph, $collection, $context);

                    return;
                }
            }

            return;
        }

        $this->saveCBD($subject, $graph, $collection, $context);
    }

    /**
     * Add $triples about a given $subject to Mongo. Only $triples with subject matching $subject will be added, others will be ignored.
     * Make them quads with a $context.
     */
    public function bsonizeTriplesAbout(string $subject, array $triples, ?string $context = null): ?array
    {
        $context = ($context == null) ? Config::getInstance()->getDefaultContextAlias() : $this->labeller->uri_to_alias($context);
        $graph = new MongoGraph();
        foreach ($triples as $triple) {
            $triple = rtrim($triple);

            $parts = $this->splitTriple($triple);
            $subject = trim($parts[0], '><');
            $predicate = trim($parts[1], '><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object)) {
                $graph->add_resource_triple($subject, $predicate, $object);
            } else {
                $graph->add_literal_triple($subject, $predicate, $object);
            }
        }

        return $graph->to_tripod_array($subject, $context);
    }

    public function extractMissingPredicateNs(array $triples): array
    {
        $missingNs = [];
        $graph = new MongoGraph();
        foreach ($triples as $triple) {
            $triple = rtrim($triple);

            $parts = $this->splitTriple($triple);
            $predicate = trim($parts[1], '><');

            try {
                $graph->uri_to_qname($predicate);
            } catch (LabellerException $te) {
                $missingNs[] = $te->getTarget();
            }
        }

        return array_unique($missingNs);
    }

    public function extractMissingObjectNs(array $triples): array
    {
        $missingNs = [];
        $graph = new MongoGraph();
        foreach ($triples as $triple) {
            $triple = rtrim($triple);

            $parts = $this->splitTriple($triple);
            $object = $this->extract_object($parts);

            if ($this->isUri($object)) {
                try {
                    $graph->uri_to_qname($object);
                } catch (LabellerException $te) {
                    $missingNs[] = $te->getTarget();
                }
            }
        }

        return array_unique($missingNs);
    }

    public function suggestPrefix(string $ns): string
    {
        $parts = preg_split('/[\/#]/', $ns) ?: [];
        for ($i = count($parts) - 1; $i >= 0; $i--) {
            if (preg_match('~^[a-zA-Z][a-zA-Z0-9\-]+$~', $parts[$i]) && $parts[$i] != 'schema' && $parts[$i] != 'ontology' && $parts[$i] != 'vocab' && $parts[$i] != 'terms' && $parts[$i] != 'ns' && $parts[$i] != 'core' && strlen($parts[$i]) > 3) {
                return strtolower($parts[$i]);
            }
        }

        return 'unknown' . uniqid();
    }

    public function getTArrayAbout(string $subject, array $triples, ?string $context): ?array
    {
        $graph = new MongoGraph();
        foreach ($triples as $triple) {
            $triple = rtrim($triple);

            $parts = $this->splitTriple($triple);
            $subject = trim($parts[0], '><');
            $predicate = trim($parts[1], '><');
            $object = $this->extract_object($parts);

            if ($this->isUri($object)) {
                $graph->add_resource_triple($subject, $predicate, $object);
            } else {
                $graph->add_literal_triple($subject, $predicate, $object);
            }
        }

        return $graph->to_tripod_array($subject, $context);
    }

    /**
     * @throws \Exception
     */
    protected function saveCBD(string $cbdSubject, MongoGraph $cbdGraph, Collection $collection, ?string $context): void
    {
        $cbdSubject = $this->labeller->uri_to_alias($cbdSubject);
        if ($cbdGraph == null || $cbdGraph->is_empty()) {
            throw new \Exception(sprintf('graph for %s was null', $cbdSubject));
        }

        try {
            $collection->insertOne($cbdGraph->to_tripod_array($cbdSubject, $context), ['w' => 1]);
            echo '.';
        } catch (\Exception $e) {
            if (preg_match('/E11000/', $e->getMessage())) {
                echo 'M';
                // key already exists, merge it
                $criteria = [_ID_KEY => [_ID_RESOURCE => $cbdSubject, _ID_CONTEXT => $context]];
                $existingGraph = new MongoGraph();
                $existingGraph->add_tripod_array($collection->findOne($criteria));
                $existingGraph->add_graph($cbdGraph);

                $collection->updateOne($criteria, ['$set' => $existingGraph->to_tripod_array($cbdSubject, $context)], ['w' => 1]);
            } else {
                // retry
                echo 'CursorException on update: ' . $e->getMessage() . ", retrying\n";

                $collection->insertOne($cbdGraph->to_tripod_array($cbdSubject, $context), ['w' => 1]);
            }
        }
    }

    private function isUri(string $object): bool
    {
        return filter_var($object, FILTER_VALIDATE_URL) !== false;
    }

    /**
     * Split an N-Triples line into its whitespace-separated parts.
     *
     * @return list<string>
     */
    private function splitTriple(string $triple): array
    {
        $parts = preg_split('/\s/', $triple);

        return $parts === false ? [] : $parts;
    }

    /**
     * @param string[] $parts
     */
    private function extract_object(array $parts): string
    {
        if (!$this->is_object_literal($parts[2])) {
            return trim($parts[2], '><');
        }

        $sliced = array_slice($parts, 2);

        $str = implode(' ', $sliced);
        $str = preg_replace('@"[^"]*$@', '', $str); // get rid of xsd typing

        $str = substr($str, 1, strlen($str) - 1); // trim($str, "\"");

        $json_string = '{"string":"' . str_replace('\u', '\u', $str) . '"}';
        $json = json_decode($json_string, true);
        if (!empty($json)) {
            return $json['string'];
        }

        return $str;
    }

    /**
     * @param string $input
     */
    private function is_object_literal($input): bool
    {
        return $input[0] == '"';
    }
}
