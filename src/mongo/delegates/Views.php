<?php

declare(strict_types=1);

namespace Tripod\Mongo\Composites;

use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\ReadPreference;
use Tripod\Config;
use Tripod\Exceptions\ViewException;
use Tripod\ITripodStat;
use Tripod\Mongo\DateUtil;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\ApplyOperation;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Timer;

class Views extends CompositeBase
{
    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there.
     */
    public function __construct(string $storeName, Collection $collection, ?string $defaultContext, ?ITripodStat $stat = null, string $readPreference = ReadPreference::PRIMARY)
    {
        $this->storeName = $storeName;
        $this->labeller = new Labeller();
        $this->collection = $collection;
        $this->podName = $collection->getCollectionName();
        $this->defaultContext = $defaultContext;
        $this->config = Config::getInstance();
        $this->stat = $stat;
        $this->readPreference = $readPreference;
    }

    public function getOperationType(): string
    {
        return OP_VIEWS;
    }

    /**
     * Receive update from subject.
     */
    public function update(ImpactedSubject $subject): void
    {
        $resource = $subject->getResourceId();
        $resourceUri = $resource[_ID_RESOURCE];
        $context = $resource[_ID_CONTEXT];

        $this->generateViews([$resourceUri], $context);
    }

    public function getTypesInSpecifications(): array
    {
        return $this->config->getTypesInViewSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * @return mixed[]
     */
    public function findImpactedComposites(array $resourcesAndPredicates, string $contextAlias): array
    {
        // This should never happen, but in the event that we have been passed an empty array or something
        if ($resourcesAndPredicates === []) {
            return [];
        }

        $contextAlias = $this->getContextAlias($contextAlias); // belt and braces

        // build a filter - will be used for impactIndex detection and finding direct views to re-gen
        $filter = [];
        $changedTypes = [];
        $typeKeys = [RDF_TYPE, $this->labeller->uri_to_alias(RDF_TYPE)];
        foreach ($resourcesAndPredicates as $resource => $predicates) {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            // build $filter for queries to impact index
            $filter[] = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];
            $rdfTypePredicates = array_intersect($predicates, $typeKeys);
            if ($rdfTypePredicates !== []) {
                $changedTypes[] = $resourceAlias;
            }
        }

        // first re-gen views where resources appear in the impact index
        $query = ['value.' . _IMPACT_INDEX => ['$in' => $filter]];

        if ($changedTypes !== []) {
            $query = ['$or' => [$query]];
            foreach ($changedTypes as $resourceAlias) {
                $query['$or'][] = [
                    _ID_KEY . '.' . _ID_RESOURCE => $resourceAlias,
                    _ID_KEY . '.' . _ID_CONTEXT => $contextAlias,
                ];
            }
        }

        $affectedViews = [];
        foreach ($this->config->getCollectionsForViews($this->storeName) as $collection) {
            $t = new Timer();
            $t->start();
            $views = $collection->find($query, ['projection' => [_ID_KEY => true]]);
            $t->stop();
            $this->timingLog(
                MONGO_FIND_IMPACTED,
                [
                    'duration' => $t->result(),
                    'query' => $query,
                    'storeName' => $this->storeName,
                    'collection' => $collection,
                ]
            );
            foreach ($views as $v) {
                $affectedViews[] = $v;
            }
        }

        return $affectedViews;
    }

    public function getSpecification(string $storeName, string $viewSpecId): ?array
    {
        return $this->config->getViewSpecification($storeName, $viewSpecId);
    }

    /**
     * Return all views, restricted by $filter conditions, for given $viewType.
     *
     * @param array $filter - an array, keyed by predicate, to filter by
     */
    public function getViews(array $filter, string $viewType): MongoGraph
    {
        $query = ['_id.type' => $viewType];
        foreach ($filter as $predicate => $object) {
            if (strpos($predicate, '$') === 0) {
                $values = [];
                foreach ($object as $obj) {
                    foreach ($obj as $p => $o) {
                        $values[] = ['value.' . _GRAPHS . '.' . $p => $o];
                    }
                }

                $query[$predicate] = $values;
            } else {
                $query['value.' . _GRAPHS . '.' . $predicate] = $object;
            }
        }

        $viewCollection = $this->getConfigInstance()->getCollectionForView($this->storeName, $viewType, $this->readPreference);

        return $this->fetchGraph($query, MONGO_VIEW, $viewCollection);
    }

    /**
     * For given $resource, return the view of type $viewType.
     */
    public function getViewForResource(?string $resource, string $viewType, ?string $context = null): MongoGraph
    {
        if (empty($resource)) {
            return new MongoGraph();
        }

        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $query = [_ID_KEY => [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias, _ID_TYPE => $viewType]];
        $viewCollection = $this->config->getCollectionForView($this->storeName, $viewType, $this->readPreference);
        $graph = $this->fetchGraph($query, MONGO_VIEW, $viewCollection);
        if ($graph->is_empty()) {
            $this->getStat()->increment(MONGO_VIEW_CACHE_MISS . ('.' . $viewType));
            $viewSpec = $this->getConfigInstance()->getViewSpecification($this->storeName, $viewType);
            if ($viewSpec == null) {
                return new MongoGraph();
            }

            $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

            $doc = $this->config->getCollectionForCBD($this->storeName, $fromCollection)
                ->findOne([_ID_KEY => [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias]]);

            if ($doc == null) {
                // if you are trying to generate a view for a document that doesnt exist in the collection
                // then we can just return an empty graph
                return new MongoGraph();
            }

            // generate view then try again
            $this->generateView($viewType, $resource, $context);

            return $this->fetchGraph($query, MONGO_VIEW, $viewCollection);
        }

        return $graph;
    }

    /**
     * For given $resources, return the views of type $viewType.
     */
    public function getViewForResources(array $resources, string $viewType, ?string $context = null): MongoGraph
    {
        $contextAlias = $this->getContextAlias($context);

        $cursorSize = 101;
        if (count($resources) > 101) {
            $cursorSize = count($resources);
        }

        $query = [_ID_KEY => ['$in' => $this->createTripodViewIdsFromResourceUris($resources, $context, $viewType)]];
        $g = $this->fetchGraph($query, MONGO_VIEW, $this->getCollectionForViewSpec($viewType), null, $cursorSize);

        // account for missing subjects
        $returnedSubjects = $g->get_subjects();
        $missingSubjects = array_diff($resources, $returnedSubjects);
        if ($missingSubjects !== []) {
            $regrabResources = [];
            foreach ($missingSubjects as $missingSubject) {
                $viewSpec = $this->getConfigInstance()->getViewSpecification($this->storeName, $viewType);
                $fromCollection = $this->getFromCollectionForViewSpec($viewSpec);

                $missingSubjectAlias = $this->labeller->uri_to_alias($missingSubject);
                $doc = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $fromCollection)
                    ->findOne([_ID_KEY => [_ID_RESOURCE => $missingSubjectAlias, _ID_CONTEXT => $contextAlias]]);

                if ($doc == null) {
                    // nothing in source CBD for this subject, there can never be a view for it
                    continue;
                }

                // generate view then try again
                $this->generateView($viewType, $missingSubject, $context);
                $regrabResources[] = $missingSubject;
            }

            if ($regrabResources !== []) {
                // only try to regrab resources if there are any to regrab
                $cursorSize = 101;
                if (count($regrabResources) > 101) {
                    $cursorSize = count($regrabResources);
                }

                $query = [_ID_KEY => ['$in' => $this->createTripodViewIdsFromResourceUris($regrabResources, $context, $viewType)]];
                $g->add_graph($this->fetchGraph($query, MONGO_VIEW, $this->getCollectionForViewSpec($viewType)));
            }
        }

        return $g;
    }

    /**
     * Autodiscovers the multiple view specification that may be applicable for a given resource, and submits each for generation.
     */
    public function generateViews(array $resources, ?string $context = null): void
    {
        $contextAlias = $this->getContextAlias($context);

        // build a filter - will be used for impactIndex detection and finding direct views to re-gen
        $filter = [];
        foreach ($resources as $resource) {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $this->debugLog('Generating views', ['store' => $this->storeName, _ID_KEY => $resourceAlias]);
            // delete any views this resource is involved in. It's type may have changed so it's not enough just to regen it with it's new type below.
            foreach ($this->getConfigInstance()->getViewSpecifications($this->storeName) as $type => $spec) {
                if ($spec['from'] == $this->podName) {
                    $this->config->getCollectionForView($this->storeName, $type, $this->readPreference)
                        ->deleteOne([_ID_KEY => [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias, _ID_TYPE => $type]]);
                }
            }

            // build $filter for queries to impact index
            $filter[] = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];
        }

        // now generate view for $resources themselves... Maybe an optimisation down the line to cut out the query here
        $query = [_ID_KEY => ['$in' => $filter]];
        $resourceAndType = $this->getCollection()->find($query, ['projection' => [_ID_KEY => 1, 'rdf:type' => 1]]);

        foreach ($resourceAndType as $rt) {
            $id = $rt[_ID_KEY];
            if (isset($rt['rdf:type'])) {
                if (isset($rt['rdf:type'][VALUE_URI])) {
                    // single type, not an array of values
                    $this->generateViewsForResourcesOfType($rt['rdf:type'][VALUE_URI], $id[_ID_RESOURCE], $id[_ID_CONTEXT]);
                } else {
                    // an array of types
                    foreach ($rt['rdf:type'] as $type) {
                        if (isset($type[VALUE_URI])) {
                            $this->generateViewsForResourcesOfType($type[VALUE_URI], $id[_ID_RESOURCE], $id[_ID_CONTEXT]);
                        }
                    }
                }
            }
        }
    }

    /**
     * This method finds all the view specs for the given $rdfType and generates the views for the $resource one by one.
     *
     * @throws \Exception
     */
    public function generateViewsForResourcesOfType(string $rdfType, ?string $resource = null, ?string $context = null): void
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;
        $viewSpecs = $this->getConfigInstance()->getViewSpecifications($this->storeName);
        foreach ($viewSpecs as $key => $viewSpec) {
            // check for rdfType and rdfTypeAlias
            if (
                ($viewSpec[_ID_TYPE] == $rdfType || (is_array($viewSpec[_ID_TYPE]) && in_array($rdfType, $viewSpec[_ID_TYPE])))
                || ($viewSpec[_ID_TYPE] == $rdfTypeAlias || (is_array($viewSpec[_ID_TYPE]) && in_array($rdfTypeAlias, $viewSpec[_ID_TYPE])))
            ) {
                $foundSpec = true;
                $this->debugLog('Processing ' . $viewSpec[_ID_KEY]);
                $this->generateView($key, $resource, $context);
            }
        }

        if (!$foundSpec) {
            $this->debugLog(sprintf("Could not find any view specifications for %s with resource type '%s'", $resource, $rdfType));

            return;
        }
    }

    /**
     * This method will delete all views where the _id.type of the viewmatches the specified $viewId.
     *
     * @param string               $viewId    View spec ID
     * @param int|UTCDateTime|null $timestamp Optional timestamp to delete all views that are older than
     *
     * @return int The number of views deleted
     */
    public function deleteViewsByViewId(string $viewId, $timestamp = null): int
    {
        $viewSpec = $this->getConfigInstance()->getViewSpecification($this->storeName, $viewId);
        if ($viewSpec == null) {
            $this->debugLog(sprintf("Could not find a view specification with viewId '%s'", $viewId));

            return 0;
        }

        $query = ['_id.type' => $viewId];
        if ($timestamp) {
            if (!$timestamp instanceof UTCDateTime) {
                $timestamp = DateUtil::getMongoDate($timestamp);
            }

            $query['$or'] = [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ];
        }

        $deleteResult = $this->getCollectionForViewSpec($viewId)
            ->deleteMany($query);

        return $deleteResult->getDeletedCount();
    }

    /**
     * Given a specific $viewId, generates a single view for the $resource.
     *
     * @param string|null $queueName Queue for background bulk generation
     *
     * @throws ViewException
     */
    public function generateView(string $viewId, ?string $resource = null, ?string $context = null, ?string $queueName = null): ?array
    {
        $contextAlias = $this->getContextAlias($context);
        $viewSpec = $this->getConfigInstance()->getViewSpecification($this->storeName, $viewId);
        if ($viewSpec == null) {
            $this->debugLog(sprintf("Could not find a view specification for %s with viewId '%s'", $resource, $viewId));

            return null;
        }

        $t = new Timer();
        $t->start();

        $from = $this->getFromCollectionForViewSpec($viewSpec);
        $collection = $this->getConfigInstance()->getCollectionForView($this->storeName, $viewId);

        if (!isset($viewSpec['joins'])) {
            throw new ViewException('Could not find any joins in view specification - usecase better served with select()');
        }

        $types = []; // this is used to filter the CBD table to speed up the view creation
        if (is_array($viewSpec[_ID_TYPE])) {
            foreach ($viewSpec[_ID_TYPE] as $type) {
                $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($type)];
                $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($type)];
            }
        } else {
            $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($viewSpec[_ID_TYPE])];
            $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($viewSpec[_ID_TYPE])];
        }

        $filter = ['$or' => $types];
        if (isset($resource)) {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $filter[_ID_KEY] = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];
        }

        // @todo Change this to a command when we upgrade MongoDB to 1.1+
        $count = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $from)->count($filter);
        $docs = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $from)->find($filter, [
            'maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout(),
        ]);

        $jobOptions = [];
        $subjects = [];
        if ($queueName && !$resource) {
            $jobOptions['statsConfig'] = $this->getStatsConfig();
            $jobGroup = $this->getJobGroup($this->storeName);
            $jobOptions[ApplyOperation::TRACKING_KEY] = $jobGroup->getId()->__toString();
            $jobGroup->setJobCount($count);
        }

        foreach ($docs as $doc) {
            if ($queueName && !$resource) {
                $subject = new ImpactedSubject(
                    $doc[_ID_KEY],
                    OP_VIEWS,
                    $this->storeName,
                    $from,
                    [$viewId]
                );
                $subjects[] = $subject;
                if (count($subjects) >= $this->getConfigInstance()->getBatchSize(OP_VIEWS)) {
                    $this->queueApplyJob($subjects, $queueName, $jobOptions);
                    $subjects = [];
                }
            } else {
                // Set up view meta information
                $generatedView = [
                    _ID_KEY => [
                        _ID_RESOURCE => $doc[_ID_KEY][_ID_RESOURCE],
                        _ID_CONTEXT => $doc[_ID_KEY][_ID_CONTEXT],
                        _ID_TYPE => $viewSpec[_ID_KEY],
                    ],
                    \_CREATED_TS => DateUtil::getMongoDate(),
                ];
                $value = []; // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R

                $value[_GRAPHS] = [];

                $buildImpactIndex = true;
                if (isset($viewSpec['ttl'])) {
                    $buildImpactIndex = false;
                    if (is_int($viewSpec['ttl']) && $viewSpec['ttl'] > 0) {
                        $value[_EXPIRES] = DateUtil::getMongoDate(
                            $this->getExpirySecFromNow($viewSpec['ttl']) * 1000
                        );
                    }
                } else {
                    $value[_IMPACT_INDEX] = [$doc[_ID_KEY]];
                }

                $this->doJoins($doc, $viewSpec['joins'], $value, $from, $contextAlias, $buildImpactIndex);

                // add top level properties
                $value[_GRAPHS][] = $this->extractProperties($doc, $viewSpec, $from);

                $generatedView['value'] = $value;

                $this->upsertGeneratedView($collection, $generatedView);
            }
        }

        if ($subjects !== []) {
            $this->queueApplyJob($subjects, $queueName, $jobOptions);
        }

        $t->stop();
        $this->timingLog(MONGO_CREATE_VIEW, [
            'view' => $viewSpec[_ID_TYPE],
            'duration' => $t->result(),
            'filter' => $filter,
            'from' => $from,
        ]);
        $this->getStat()->timer(MONGO_CREATE_VIEW . ('.' . $viewId), $t->result());

        $stat = ['count' => $count];
        if (isset($jobOptions[ApplyOperation::TRACKING_KEY])) {
            $stat[ApplyOperation::TRACKING_KEY] = $jobOptions[ApplyOperation::TRACKING_KEY];
        }

        return $stat;
    }

    /**
     * Count the number of documents in the spec that match $filters.
     *
     * @param string               $viewSpec View spec ID
     * @param array<string, mixed> $filters  Query filters to get count on
     */
    public function count(string $viewSpec, array $filters = []): int
    {
        $filters['_id.type'] = $viewSpec;

        return $this->getCollectionForViewSpec($viewSpec)->count($filters);
    }

    protected function getExpirySecFromNow(int $secs): int
    {
        return time() + $secs;
    }

    /**
     * Joins data to $dest from $source according to specification in $joins, or queries DB if data is not available in $source.
     */
    protected function doJoins(array $source, array $joins, array &$dest, string $from, string $contextAlias, bool $buildImpactIndex = true): void
    {
        // expand sequences before doing any joins...
        $this->expandSequence($joins, $source);

        foreach ($joins as $predicate => $ruleset) {
            if ($predicate == 'followSequence') {
                continue;
            }

            if (isset($source[$predicate])) {
                // todo: perhaps we can get better performance by detecting whether or not
                // the uri to join on is already in the impact index, and if so not attempting
                // to join on it. However, we need to think about different combinations of
                // nested joins in different points of the view spec and see if this would
                // complicate things. Needs a unit test or two.
                $joinUris = [];
                if (isset($source[$predicate][VALUE_URI])) {
                    // single value for join
                    $joinUris[] = [_ID_RESOURCE => $source[$predicate][VALUE_URI], _ID_CONTEXT => $contextAlias];
                } elseif ($predicate == _ID_KEY) {
                    $joinUris[] = [_ID_RESOURCE => $source[$predicate][_ID_RESOURCE], _ID_CONTEXT => $contextAlias];
                } else {
                    // multiple values for join
                    $joinsPushed = 0;
                    foreach ($source[$predicate] as $v) {
                        if (isset($ruleset['maxJoins']) && !$joinsPushed < $ruleset['maxJoins']) {
                            break; // maxJoins reached
                        }

                        $joinUris[] = [_ID_RESOURCE => $v[VALUE_URI], _ID_CONTEXT => $contextAlias];
                        $joinsPushed++;
                    }
                }

                $recursiveJoins = [];
                $collection = (
                    isset($ruleset['from'])
                    ? $this->config->getCollectionForCBD($this->storeName, $ruleset['from'])
                    : $this->config->getCollectionForCBD($this->storeName, $from)
                );

                $cursor = $collection->find([_ID_KEY => ['$in' => $joinUris]], [
                    'maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout(),
                ]);

                $this->addIdToImpactIndex($joinUris, $dest, $buildImpactIndex);
                foreach ($cursor as $linkMatch) {
                    // if there is a condition, check it...
                    if (isset($ruleset['condition'])) {
                        $ruleset['condition']['._id'] = $linkMatch[_ID_KEY];
                    }

                    if (!(isset($ruleset['condition']) && $collection->count($ruleset['condition']) == 0)) {
                        // make sure any sequences are expanded before extracting properties
                        if (isset($ruleset['joins'])) {
                            $this->expandSequence($ruleset['joins'], $linkMatch);
                        }

                        if (isset($ruleset['filter'])) {
                            foreach ($ruleset['filter'] as $filterPredicate => $filter) {
                                foreach ($filter as $filterType => $filterMatch) {
                                    if (isset($linkMatch[$filterPredicate])) {
                                        foreach ($linkMatch[$filterPredicate] as $linkMatchType => $linkMatchValues) {
                                            if (is_array($linkMatchValues) === false) {
                                                $linkMatchValues = [$linkMatchType => $linkMatchValues];
                                            }

                                            foreach ($linkMatchValues as $linkMatchType => $linkMatchValue) {
                                                if ($this->matchesFilter($linkMatchType, $linkMatchValue, $filterType, $filterMatch)) {
                                                    $dest[_GRAPHS][] = $this->extractProperties($linkMatch, $ruleset, $from);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            $dest[_GRAPHS][] = $this->extractProperties($linkMatch, $ruleset, $from);
                        }

                        if (isset($ruleset['joins'])) {
                            // recursive joins must be done after this cursor has completed, otherwise things get messy
                            $recursiveJoins[] = ['data' => $linkMatch, 'ruleset' => $ruleset['joins']];
                        }
                    }
                }

                foreach ($recursiveJoins as $r) {
                    $this->doJoins($r['data'], $r['ruleset'], $dest, $from, $contextAlias, $buildImpactIndex);
                }
            }
        }
    }

    /**
     * Check to see if a linkMatch matches a filter.
     */
    protected function matchesFilter(string $linkMatchType, string $linkMatchValue, string $filterType, string $filterMatch): bool
    {
        if ($linkMatchType !== $filterType) {
            return false;
        }

        return $linkMatchValue === $filterMatch || $this->labeller->uri_to_alias($linkMatchValue) === $filterMatch;
    }

    /**
     * Returns a document with properties extracted from $source, according to $viewSpec. Useful for partial representations
     * of CBDs in a view.
     *
     * @param array<string, mixed> $source
     * @param array<string, mixed> $viewSpec
     */
    protected function extractProperties(array $source, array $viewSpec, string $from): array
    {
        $obj = [];
        if (isset($viewSpec['include'])) {
            $obj[_ID_KEY] = $source[_ID_KEY];
            foreach ($viewSpec['include'] as $p) {
                if (isset($source[$p])) {
                    $obj[$p] = $source[$p];
                }

                if ($p === INCLUDE_RDF_SEQUENCE && $source['rdf:type']) {
                    foreach ($source['rdf:type'] as $u => $t) {
                        if (is_array($t) === false) {
                            $t = [$u => $t];
                        }

                        foreach ($t as $typeOfType => $type) {
                            if ($typeOfType === VALUE_URI && $type === 'rdf:Seq') {
                                $seqNumber = 1;
                                $found = true;
                                while ($found) {
                                    if (isset($source['rdf:_' . $seqNumber])) {
                                        $obj['rdf:_' . $seqNumber] = $source['rdf:_' . $seqNumber];
                                        $seqNumber++;
                                    } else {
                                        $found = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (isset($viewSpec['joins'])) {
                foreach ($viewSpec['joins'] as $p => $join) {
                    if (isset($join['maxJoins'])) {
                        // todo: refactor with below (extract method)
                        // only include up to maxJoins
                        for ($i = 0; $i < $join['maxJoins']; $i++) {
                            if (isset($source[$p]) && (isset($source[$p][VALUE_URI]) || isset($source[$p][VALUE_LITERAL])) && $i === 0) { // cater for source with only one val
                                $obj[$p] = $source[$p];
                            }

                            if (isset($source[$p][$i])) {
                                if (!isset($obj[$p])) {
                                    $obj[$p] = [];
                                }

                                $obj[$p][] = $source[$p][$i];
                            }
                        }
                    } elseif (isset($source[$p])) {
                        $obj[$p] = $source[$p];
                    }
                }
            }
        } else {
            foreach ($source as $p => $val) {
                if (isset($viewSpec['joins'][$p]['maxJoins'])) {
                    // todo: refactor with above (extract method)
                    // only include up to maxJoins
                    for ($i = 0; $i < $viewSpec['joins'][$p]['maxJoins']; $i++) {
                        if ($val && (isset($val[VALUE_URI]) || isset($val[VALUE_LITERAL])) && $i === 0) { // cater for source with only one val
                            $obj[$p] = $val;
                        }

                        if ($val && isset($val[$i])) {
                            if (!isset($obj[$p])) {
                                $obj[$p] = [];
                            }

                            $obj[$p][] = $val[$i];
                        }
                    }
                } else {
                    $obj[$p] = $val;
                }
            }
        }

        // process count aggregate function
        if (isset($viewSpec['counts'])) {
            foreach ($viewSpec['counts'] as $predicate => $c) {
                if (isset($c['filter'])) { // run a db filter
                    $collection = (
                        isset($c['from'])
                        ? $this->config->getCollectionForCBD($this->storeName, $c['from'])
                        : $this->config->getCollectionForCBD($this->storeName, $from)
                    );
                    $query = $c['filter'];
                    $query[$c['property'] . '.' . VALUE_URI] = $source[_ID_KEY][_ID_RESOURCE]; // todo: how does graph restriction work here?
                    $obj[$predicate] = [VALUE_LITERAL => $collection->count($query) . '']; // make sure it's a string
                } else { // just look for property in current source...
                    $count = 0;
                    // just count predicates at current location
                    if (isset($source[$c['property']])) {
                        if (isset($source[$c['property']][VALUE_URI]) || isset($source[$c['property']][VALUE_LITERAL])) {
                            $count = 1;
                        } else {
                            $count = count($source[$c['property']]);
                        }
                    }

                    $obj[$predicate] = [VALUE_LITERAL => (string) $count];
                }
            }
        }

        return $obj;
    }

    protected function getCollectionForViewSpec(string $viewSpecId): Collection
    {
        return $this->getConfigInstance()->getCollectionForView($this->storeName, $viewSpecId);
    }

    private function createTripodViewIdsFromResourceUris(array $resourceUriOrArray, ?string $context, string $viewType): array
    {
        $contextAlias = $this->getContextAlias($context);
        $ret = [];
        foreach ($resourceUriOrArray as $resource) {
            $ret[] = [_ID_RESOURCE => $this->labeller->uri_to_alias($resource), _ID_CONTEXT => $contextAlias, _ID_TYPE => $viewType];
        }

        return $ret;
    }

    /**
     * @param array<string, mixed>|null $viewSpec
     *
     * @return string
     */
    private function getFromCollectionForViewSpec(?array $viewSpec)
    {
        return $viewSpec['from'] ?? $this->podName;
    }

    private function upsertGeneratedView(Collection $collection, array $generatedView): void
    {
        try {
            $collection->replaceOne([_ID_KEY => $generatedView[_ID_KEY]], $generatedView, ['upsert' => true]);
        } catch (BulkWriteException $e) {
            if ($this->isDuplicateKeyError($e)) {
                $existingView = $collection->findOne([_ID_KEY => $generatedView[_ID_KEY]]);
                $this->getLogger()->warning('Duplicate key error when upserting generated view, retrying.', [
                    'error' => $e,
                    'generatedView' => $generatedView,
                    'existingView' => $existingView,
                ]);
                $collection->replaceOne([_ID_KEY => $generatedView[_ID_KEY]], $generatedView, ['upsert' => false]);
            } else {
                throw $e;
            }
        }
    }

    private function isDuplicateKeyError(BulkWriteException $error): bool
    {
        return $error->getCode() === 11000 || strpos($error->getMessage(), 'E11000') !== false;
    }
}
