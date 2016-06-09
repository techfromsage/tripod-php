<?php

namespace Tripod\Mongo\Jobs;

use \Tripod\Mongo\Config;
/**
 * Class DiscoverImpactedSubjects
 *
 * This job finds documents in composite collections whose specs are out-of-date. It should be periodically triggered (e.g. every five minutes) by an external service (e.g. CRON).
 *
 * The job will find all composite specifications, and for each of those, run a query to find a set of outdated documents.
 *
 * For a config with S specifications, each `perform()` call will therefore run S independent queries, each of which may
 * return up to CURSOR_LIMIT documents to update, thus regenerating a maximum of `S*CURSOR_LIMIT` composite documents.
 * 
 * There are a number of things left TODO, as later considerations:
 * 
 * [1] Randomise the order of the specs, to avoid contention 
 *     (e.g. one heavily-used view sucking up all regens in cursor)
 *
 * [2] Reduce number of queries. Currently, it fetches a collection for every composite spec, 
 *     even when many composites will share the same collection (e.g. db.views on data1, or 
 *     db.table_rows on data2). It should be possible to group by datasource/collection pair.
 *     That would reduce the number of queries from |specifications| to |<datasource,collection>|
 *
 * @package Tripod\Mongo\Jobs
 */
class DiscoverOutdatedComposites extends JobBase {

    const STORE_NAME_KEY = 'storeName';
    const CURSOR_LIMIT = 'cursorLimit';

    /**
     * Run the DiscoverOutdatedComposites job
     * @throws \Exception
     */
    public function perform()
    {
        $this->debugLog("DiscoverOutdatedComposites::perform() start");

        $this->validateArgs();

        $config = $this->args[self::TRIPOD_CONFIG_KEY];
        $storeName = $this->args[self::STORE_NAME_KEY];
        $cursorLimit = $this->args[self::CURSOR_LIMIT];

        // set the config to what is received
        \Tripod\Mongo\Config::setConfig($config);

        // closure around $cursorLimit in $this->getRegenTaskForMetadata
        $getRegenTaskForMetadata = function($metadatum) use ($cursorLimit) { 
            return $this->getRegenTaskForMetadata($metadatum, $cursorLimit); 
        };

        // compile a list of all composite configuration metadata objects
        $compositeMetadata = $this->getCompositeMetadata($config, $storeName);

        // for each composite metadata instance, fetch regen tasks (where they exist), 
        // which include specific CBDs to regenerate
        $regenTasksIncludingNulls = array_map($getRegenTaskForMetadata, $compositeMetadata);
        $regenTasks = array_filter($regenTasksIncludingNulls, 'isset');

        // for collected regeneration tasks, run the update
        // TODO: this will probably want to schedule updates, rather than running directly
        $this->runRegenerationTasks($regenTasks);
    }

    /**
     * @param array $data
     * @param string|null $queueName
     */
    public function createJob(Array $data, $queueName=null)
    {
        if(!$queueName)
        {
            $queueName = Config::getDiscoverQueueName();
        }
        elseif(strpos($queueName, \Tripod\Mongo\Config::getDiscoverQueueName()) === false)
        {
            $queueName = \Tripod\Mongo\Config::getDiscoverQueueName() . '::' . $queueName;
        }
        $this->submitJob($queueName,get_class($this),$data);
    }

    /**
     * Validate arguments in the job message
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array(
            self::TRIPOD_CONFIG_KEY,
            self::STORE_NAME_KEY,
            self::CURSOR_LIMIT
        );
    }

    /**
    * Given an array of regeneration tasks, runs all those tasks.
    *
    * @param array $regenTasks
    **/
    public function runRegenerationTasks($regenTasks) {
        foreach ($regenTasks as $regenTask) {
            $specification = $regenTask->specification;
            $compositeCollection = $regenTask->compositeCollection;
            $compositeRegenFunction = $regenTask->compositeRegenFunction;
            
            // regenerate individual composites from root CBDs and specifications
            foreach($regenTask->cbdDocuments as $cbdDoc) {
                $compositeRegenFunction($specification, $compositeCollection, $cbdDoc);
            }
        }
    }

    /**
     * Generates regeneration task data for any outdated documents in a composite collection,
     * by scanning that composite collection one cursor-page at a time.
     *
     * @param CompositeMetadata $metadatum - the composite collection to search
     * @return CompositeRegenTask|null - a CompositeRegenTask, if outdated documents are found; null otherwise
    **/
    public function getRegenTaskForMetadata($metadatum, $cursorLimit) {
        $filterOutdated = $metadatum->getOutdatedQueryComponent();
        if(!isset($filterOutdated)) {
            return null;
        } else {
            $compositeCollection = $metadatum->compositeCollection;
            $specification = $metadatum->specification;
            $cbdCollection = $metadatum->cbdCollection;
            $compositeRegenFunction = $metadatum->compositeRegenFunction;

            $cbdIdFields = array(_ID_KEY.'.'._ID_RESOURCE => 1, _ID_KEY.'.'._ID_CONTEXT => 1);

            // find one cursor page at a time for this composite
            $outdatedComposites = 
                $compositeCollection
                    ->find($filterOutdated, $cbdIdFields)
                    ->limit($cursorLimit);

            if(!$outdatedComposites->hasNext()) {
                // no outdated composite documents found, so no work to be done
                return null;
            } else {
                // find IDs root CBDs required to regenerate the composite
                $outdatedCbdIds = 
                    array_map(array($this, 'composite2cbdId'), iterator_to_array($outdatedComposites, false));

                // fetch the CBDs themselves
                $filterCbdsById = array('$or' => $outdatedCbdIds);
                $cbdDocs = $cbdCollection->find($filterCbdsById);

                if(!$cbdDocs->hasNext()) {
                    // no CBDs found, so nothing to regenerate
                    return null;
                } else {
                    // return a task to regenerate outdated composite documents
                    return new CompositeRegenTask(
                        $compositeRegenFunction, 
                        $specification, 
                        $compositeCollection, 
                        iterator_to_array($cbdDocs, false)
                    );
                }
            }
        }
    }

    // TODO: this should really live in IComposite implementations, but cannot yet.
    public function makeViewRegenFunc($storeName) {
        // TODO: remove $views from here. We need it because that class provides a method for
        // regenerating indvidual views.  However, that should probably be abstracted somewhere,
        // into a method capable of regenerating individual composites, by type.

        // TODO: these fakes are only here to satisfy Views, which probably does not
        // need either of these as constructor arguments (most methods in Views are agnostic)
        $fakeCollection = \Tripod\Mongo\Config::getInstance()->getCollectionForCBD($storeName, 'CBD_testing');
        $fakeDefaultContext = 'http://talisaspire.com/';

        $views = new \Tripod\Mongo\Composites\Views(
            $storeName,
            $fakeCollection,
            $fakeDefaultContext
        );

        return function($spec, $compositeCollection, $cbdDoc) use ($fakeDefaultContext, $views) {
            $views->saveGeneratedView($spec, $compositeCollection, $cbdDoc, $spec['from'], $fakeDefaultContext);
        };
    }

    /**
    * Gets a list of all CompositeMetadata objects, one per composite in the specification.
    *
    * @param \Config $config - the configuration instance
    * @param string $storeName
    * @return array - an array of CompositeMetadata objects
    **/
    public function getCompositeMetadata($config, $storeName) {
        // for a given composite type and specification, collect the metadata we need to run queries
        $composite2metadata = function($compositeType, $spec, $regenFunc) use ($config, $storeName) {
            $compositeCollection = 
                $config->getCollectionForCompositeType($compositeType, $storeName, $spec[_ID_KEY]);
            $cbdCollection = $config->getFromCollectionForSpec($storeName, $spec);
            return new CompositeMetadata(
                $compositeType, 
                $spec, 
                $compositeCollection, 
                $cbdCollection,
                $regenFunc
            );
        };

        // views
        $viewRegen = $this->makeViewRegenFunc($storeName);
        $view2metadata = function($spec) use ($composite2metadata, $viewRegen) {
            return $composite2metadata(COMPOSITE_TYPE_VIEWS, $spec, $viewRegen);
        };

        $viewMetadata = array_map($view2metadata, $config->getViewSpecifications($storeName));

        // TODO: bring in the other composite types, too
        return $viewMetadata;
    }

    /**
    * Converts a composite document into an ID for finding its root CBD.
    *
    * @param array $compositeDocument - the composite being examined
    * @return array - the ID of $compositeDocument's root CBD.
    **/
    protected function composite2cbdId($compositeDocument) {
        // these must be valid - they come from a valid composite
        $cbdResourceAlias = $compositeDocument[_ID_KEY][_ID_RESOURCE];
        $cbdContextAlias = $compositeDocument[_ID_KEY][_ID_CONTEXT];
        return array(_ID_KEY => 
            array(_ID_RESOURCE => $cbdResourceAlias, _ID_CONTEXT => $cbdContextAlias));
    }
}

/**
* Represents data about a particular kind of composite, and provides tools for regenerating documents.
**/
class CompositeMetadata {
    public $compositeType;
    public $specification;
    public $compositeCollection;
    public $cbdCollection;
    public $compositeRegenFunction;

    public function __construct($compositeType, $specification, $compositeCollection, $cbdCollection, $compositeRegenFunction)
    {
        $this->compositeType = $compositeType;
        $this->specification = $specification;
        $this->compositeCollection = $compositeCollection;
        $this->cbdCollection = $cbdCollection;
        $this->compositeRegenFunction = $compositeRegenFunction;
    }

    /**
    * Create a query component for finding composites matching matching the specification,
    * but which are outdated.
    *
    * The mongodb equivalent is:
    *
    * { "_spec.type": <THIS TYPE>, "_spec.revision": { "$lt": <SPEC REVISION NUMBER> }}
    *
    * ALTERNATIVE: This is predicated on having independent revisions for each composite spec. However,
    * It would also be possible to run this query using a single global revision and a composite spec hash:
    *
    * { 
    *   "_spec.type": <THIS TYPE>, 
    *   "_spec.revision": { "$lt": <GLOBAL REVISION NUMBER> }, 
    *   "_spec.hash": { "$ne": <HASH OF THE SPEC> }
    * }
    *
    * That alternative would make managing the revision numbers much more straightforward, since we can simply
    * Increment that global number on every build, and the hash will tell us if an update is required. Crucially,
    * The presence of _spec.revision will impose order, so we don't end up playing tennis between two clients with
    * different versions.
    *
    * @return array - the query compontent used to find outdated documents for this composite.
    **/
    public function getOutdatedQueryComponent() {
        if(isset($this->specification[_REVISION])) {
            $specType = _SPEC_KEY.'.'._SPEC_TYPE;
            $specRevision = _SPEC_KEY.'.'._SPEC_REVISION;
            return array(
                $specType => $this->specification[_ID_KEY],
                $specRevision => array('$lt' => $this->specification[_REVISION])
            );
        } else {
            return null;
        }
    }
}

/**
* Represents a task for regenerating a number of composites.
* 
* Contains enough information on the composite collection, and a list of root CBDs,
* which are used to regenerate outdated composite documents.
**/
class CompositeRegenTask {
    public $compositeRegenFunction;
    public $specification;
    public $compositeCollection;
    public $cbdDocuments;

    public function __construct($compositeRegenFunction, $specification, $compositeCollection, $cbdDocuments)
    {
        $this->compositeRegenFunction = $compositeRegenFunction;
        $this->specification = $specification;
        $this->compositeCollection = $compositeCollection;
        $this->cbdDocuments = $cbdDocuments;
    }
}