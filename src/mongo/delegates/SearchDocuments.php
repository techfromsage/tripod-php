<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Collection;
use MongoDB\Driver\ReadPreference;
use Tripod\ITripodStat;
use Tripod\Timer;

class SearchDocuments extends DriverBase
{
    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there.
     */
    public function __construct(string $storeName, Collection $collection, string $defaultContext, ?ITripodStat $stat = null, string $readPreference = ReadPreference::PRIMARY)
    {
        $this->labeller = new Labeller();
        $this->storeName = $storeName;
        $this->collection = $collection;
        $this->podName = $collection->getCollectionName();
        $this->defaultContext = $defaultContext;
        $this->stat = $stat;
        $this->readPreference = $readPreference;
    }

    /**
     * @throws \Exception
     */
    public function generateSearchDocumentBasedOnSpecId(string $specId, ?string $resource, ?string $context): ?array
    {
        if (empty($resource)) {
            throw new \Exception('Resource must be specified');
        }

        if (empty($context)) {
            throw new \Exception('Context must be specified');
        }

        $searchSpec = $this->getSearchDocumentSpecification($specId);
        if (empty($searchSpec)) {
            $this->debugLog('Could not find Search Document Specification for ' . $specId);

            return null;
        }

        $from = $searchSpec['from'] ?? $this->podName;

        // work out whether or not to index at all
        $proceedWithGeneration = false;

        foreach ($searchSpec['filter'] as $indexRules) {
            // run a query to work out
            if (!empty($indexRules['condition'])) {
                $irFrom = (empty($indexRules['from'])) ? $this->podName : $indexRules['from'];
                // add id of current record to rules..
                $indexRules['condition'][_ID_KEY] = [
                    _ID_RESOURCE => $this->labeller->uri_to_alias($resource),
                    _ID_CONTEXT => $this->labeller->uri_to_alias($context),
                ];

                if ($this->getConfigInstance()->getCollectionForCBD($this->storeName, $irFrom)->findOne($indexRules['condition'])) {
                    // match found, add this spec id to those that should be generated
                    $proceedWithGeneration = true;
                }
            } else {
                // no restriction rules, so just add to generate
                $proceedWithGeneration = true;
            }
        }

        if ($proceedWithGeneration === false) {
            $this->debugLog(sprintf('Unable to proceed with generating %s search document for %s, does not satisfy rules', $specId, $resource));

            return null;
        }

        $_id = [
            _ID_RESOURCE => $this->labeller->uri_to_alias($resource),
            _ID_CONTEXT => $this->labeller->uri_to_alias($context),
        ];

        $sourceDocument = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $from)->findOne([_ID_KEY => $_id]);

        if (empty($sourceDocument)) {
            $this->debugLog(sprintf('Source document not found for %s, cannot proceed generating %s search document', $resource, $specId));

            return null;
        }

        $this->debugLog('Processing ' . $specId);

        // build the document
        $generatedDocument = [\_CREATED_TS => DateUtil::getMongoDate()];
        $this->addIdToImpactIndex($_id, $generatedDocument);

        $_id[_ID_TYPE] = $specId;
        $generatedDocument[_ID_KEY] = $_id;

        if (isset($searchSpec['fields'])) {
            $this->addFields($sourceDocument, $searchSpec['fields'], $generatedDocument);
        }

        if (isset($searchSpec['indices'])) {
            $this->addFields($sourceDocument, $searchSpec['indices'], $generatedDocument, true);
        }

        if (isset($searchSpec['joins'])) {
            $this->doJoin($sourceDocument, $searchSpec['joins'], $generatedDocument, $from);
        }

        return $generatedDocument;
    }

    public function generateSearchDocumentsBasedOnRdfTypes(array $rdfTypes, string $resource, string $context): array
    {
        // this is what is returned
        $generatedSearchDocuments = [];

        $timer = new Timer();
        $timer->start();

        foreach ($rdfTypes as $rdfType) {
            $specs = $this->getConfigInstance()->getSearchDocumentSpecifications($this->storeName, $rdfType);

            if (empty($specs)) {
                continue;
            } // no point doing anything else if there is no spec for the type

            foreach ($specs as $searchSpec) {
                $generatedSearchDocuments[] = $this->generateSearchDocumentBasedOnSpecId($searchSpec[_ID_KEY], $resource, $context);
            }
        }

        $timer->stop();

        // echo "\n\tTook " . $timer->result() . " ms to generate search documents\n";
        return $generatedSearchDocuments;
    }

    public function getSearchCollectionName(): string
    {
        return SEARCH_INDEX_COLLECTION;
    }

    protected function doJoin(array $source, array $joins, array &$target, string $from): void
    {
        // expand sequences before proceeding
        $this->expandSequence($joins, $source);
        $config = $this->getConfigInstance();
        foreach ($joins as $predicate => $rules) {
            if (isset($source[$predicate])) {
                $joinUris = [];

                if (isset($source[$predicate][VALUE_URI])) {
                    // single value for join
                    $joinUris[] = [_ID_RESOURCE => $source[$predicate][VALUE_URI], _ID_CONTEXT => $this->defaultContext]; // todo: check that default context is the right thing to set here and below
                } else {
                    // multiple values for join
                    foreach ($source[$predicate] as $v) {
                        $joinUris[] = [_ID_RESOURCE => $v[VALUE_URI], _ID_CONTEXT => $this->defaultContext];
                    }
                }

                $recursiveJoins = [];

                $collection = (
                    isset($rules['from'])
                    ? $config->getCollectionForCBD($this->storeName, $rules['from'])
                    : $config->getCollectionForCBD($this->storeName, $from)
                );

                $cursor = $collection->find([_ID_KEY => ['$in' => $joinUris]], [
                    'maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout(),
                ]);

                // add to impact index
                $this->addIdToImpactIndex($joinUris, $target);
                foreach ($cursor as $linkMatch) {
                    if (isset($rules['fields'])) {
                        $this->addFields($linkMatch, $rules['fields'], $target);
                    }

                    if (isset($rules['indices'])) {
                        $this->addFields($linkMatch, $rules['indices'], $target, true);
                    }

                    if (isset($rules['join'])) {
                        $recursiveJoins[] = ['data' => $linkMatch, 'ruleset' => $rules['joins']];
                    }
                }

                foreach ($recursiveJoins as $rj) {
                    $this->doJoin($rj['data'], $rj['ruleset'], $target, $from);
                }
            }
        }
    }

    protected function addFields(array $source, array $fieldsOrIndices, array &$target, bool $isIndex = false): void
    {
        foreach ($fieldsOrIndices as $f) {
            if (isset($f['predicates'])) {
                $predicates = $f['predicates'];
                foreach ($predicates as $p) {
                    $values = [];

                    if (isset($source[$p])) {
                        $sourceValue = $source[$p];
                        if (isset($sourceValue[VALUE_URI])) {
                            $values[] = $this->normalizeSearchValue($sourceValue[VALUE_URI], $isIndex);
                        } elseif (isset($sourceValue[VALUE_LITERAL])) {
                            $values[] = $this->normalizeSearchValue($sourceValue[VALUE_LITERAL], $isIndex);
                        } elseif (is_array($sourceValue)) {
                            foreach ($sourceValue as $v) {
                                if (!is_array($v)) {
                                    continue;
                                }
                                if (isset($v[VALUE_URI])) {
                                    $values[] = $this->normalizeSearchValue($v[VALUE_URI], $isIndex);
                                } elseif (isset($v[VALUE_LITERAL])) {
                                    $values[] = $this->normalizeSearchValue($v[VALUE_LITERAL], $isIndex);
                                }
                            }
                        }
                    }

                    // now add the values
                    $this->addValuesToTarget($values, $f, $target);
                }
            }

            if (isset($f['value'])) {
                $values = [];

                if ($f['value'] == '_link_' || $f['value'] == 'link') {
                    if ($f['value'] == '_link_') {
                        $this->warningLog("Search spec value '_link_' is deprecated", $f);
                    }

                    $values[] = $this->labeller->qname_to_alias($source[_ID_KEY][_ID_RESOURCE]);
                }

                $this->addValuesToTarget($values, $f, $target);
            }
        }
    }

    protected function getSearchDocumentSpecification(string $specId): ?array
    {
        return $this->getConfigInstance()->getSearchDocumentSpecification($this->storeName, $specId);
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    private function normalizeSearchValue($value, bool $isIndex)
    {
        $value = trim(is_scalar($value) ? (string) $value : '');

        return $isIndex ? mb_strtolower($value, 'UTF-8') : $value;
    }

    private function addValuesToTarget(array $values, array $field, array &$target): void
    {
        $objName = null;
        $name = $field['fieldName'];

        if (strpos($name, '.') !== false) {
            $parts = explode('.', $name);
            $objName = $parts[0];
            $name = $parts[1];
        }

        $limit = $field['limit'] ?? count($values);

        if ($values !== []) {
            for ($i = 0; $i < $limit; $i++) {
                $v = $values[$i];
                if (in_array($objName, [null, '', '0'], true)) {
                    if (!isset($target[$name])) {
                        $target[$name] = $v;
                    } elseif (is_array($target[$name])) {
                        $target[$name][] = $v;
                    } else {
                        $existingVal = $target[$name];
                        $target[$name] = [];
                        $target[$name][] = $existingVal;
                        $target[$name][] = $v;
                    }
                } elseif (!isset($target[$objName][$name])) {
                    $target[$objName][$name] = $v;
                } elseif (is_array($target[$objName][$name])) {
                    $target[$objName][$name][] = $v;
                } else {
                    $existingVal = $target[$objName][$name];
                    $target[$objName][$name] = [];
                    $target[$objName][$name][] = $existingVal;
                    $target[$objName][$name][] = $v;
                }
            }
        }
    }
}
