<?php

namespace Tripod\Mongo\Jobs;

use \Tripod\Mongo\Config;
/**
 * Class DiscoverImpactedSubjects
 *
 * This job finds documents in composite collections whose specs are out-of-date. It should be periodically triggered.
 *
 * The job will find all composite specifications, and for each of those, run a query to find a set of outdated documents.
 *
 * For a config with S specifications, each `perform()` call will therefore run S independent queries, each of which may
 * return up to CURSOR_LIMIT documents to update, thus regenerating a maximum of `S*CURSOR_LIMIT` composite documents.
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

        // TODO: remove these fakes. Only here to satisfy Views, which probably does not
        // need either of these as constructor arguments.
        $fakeCollection = null;
        $fakeDefaultContext = null;

        $views = new \Tripod\Mongo\Composites\Views(
            $storeName,
            $fakeCollection,
            $fakeDefaultContext
        );

        // closure around $cursorLimit in $this->getRegenTasksForMetadata
        $getRegenTasksForMetadata = function($metadatum) use ($cursorLimit) { 
            return $this->getRegenTasksForMetadata($metadatum, $cursorLimit); 
        };

        // TODO: optimisations... this currently fetches a collection for every composite spec, even when many 
        // composites will share the same collection (e.g. db.views on data1, or db.table_rows on data2).
        // It should be possible to compose each individual query into an '$or' query across all $metadatum 
        // instances that share the same datasource/collection pair.
        // That would reduce the number of queries from |specifications| to |<datasource,collection>| pairs.

        // compile a list of all composite configuration metadata objects
        $compositeMetadata = $this->getCompositeMetadata($config, $storeName);

        // for each composite metadata instance, fetch regen tasks (where they exist), which include specific CBDs to regenerate
        $regenTasksIncludingNulls = array_map($getRegenTasksForMetadata, $compositeMetadata);
        $regenTasks = array_filter($regenTasksIncludingNulls, 'isset');

        // for collected regeneration tasks, run the update
        $this->runRegenerationTasks($regenTasks, $views);
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

    protected function runRegenerationTasks($regenTasks, $views) {
        foreach ($regenTasks as $regenTask) {
            $spec = $regenTask->specification;
            $compositeCollection = $regenTask->compositeCollection;
            foreach($cbdDocs as $cbdDoc) {
                $this->regenerateComposite($specification, $compositeCollection, $cbdDoc, $views);
            }
        }
    }

    protected function getRegenTasksForMetadata($metadatum, $cursorLimit) {
        $compositeCollection = $metadatum->compositeCollection;
        $specification = $metadatum->specification;
        $cbdCollection = $metadatum->cbdCollection;

        $filterOutdated = $metadatum->getOutdatedQueryComponent();

        if(!isset($filterOutdated)) {
            return null;
        } else {
            // find one cursor page at a time for this composite
            $outdatedComposites = 
                $compositeCollection
                    // TODO: select only the fields we need from the database, 
                    // not entire composite document; just array(_ID_KEY)
                    ->find($filterOutdated)
                    ->limit($cursorLimit);

            $outdatedCbdIds = array_map($this->composite2cbdId, $outdatedComposites);
            $filterCbdsById = array('$or' => $outdatedCbdIds);
            $cbdDocs = $cbdCollection->find($filterCbdsById);

            return new CbdRegeneration($specification, $compositeCollection, $cbdDocs);
        }
    }

    public function getCompositeMetadata($config, $storeName) {
        // for a given view specification, collect the metadata we need to run queries
        $view2metadata = function($spec) use ($config, $storeName) {
            $viewCollection = $config->getCollectionForView($storeName, $spec[_ID_KEY]);
            $cbdCollection = $config->getFromCollectionForSpec($storeName, $spec);
            return new CompositeMetadata(OP_VIEWS, $spec, $viewCollection, $cbdCollection);
        };

        $viewMetadata = array_map($view2metadata, $config->getViewSpecifications($storeName));

        // TODO: bring in the other composite types, too
        return $viewMetadata;
    }

    protected function composite2cbdId($compositeDocument) {
        // these must be valid - they come from a valid composite
        $cbdResourceAlias = $compositeDocument[_ID_KEY][_ID_RESOURCE];
        $cbdContextAlias = $compositeDocument[_ID_KEY][_ID_CONTEXT];
        return array(_ID_KEY => 
            array(_ID_RESOURCE => $cbdResourceAlias, _ID_CONTEXT => $cbdContextAlias));
    }

    // TODO: this is limited to views at present. Abstract it.
    protected function regenerateComposite($spec, $compositeCollection, $cbdDoc, $views, $context=null) {
        // regenerate this composite from the CBD
        $contextAlias = $this->getContextAlias($context);
        $views->saveGeneratedView($spec, $compositeCollection, $cbdDoc, $spec['from'], $contextAlias);
    }

}

class CompositeMetadata {
    public $compositeType;
    public $specification;
    public $compositeCollection;
    public $cbdCollection;

    public function __construct($compositeType, $specification, $compositeCollection, $cbdCollection)
    {
        $this->compositeType = $compositeType;
        $this->specification = $specification;
        $this->compositeCollection = $compositeCollection;
        $this->cbdCollection = $cbdCollection;
    }

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


class CbdRegeneration {
    public $specification;
    public $compositeCollection;
    public $cbdDocuments;

    public function __construct($specification, $compositeCollection, $cbdDocuments)
    {
        $this->specification = $specification;
        $this->compositeCollection = $compositeCollection;
        $this->cbdDocuments = $cbdDocuments;
    }
}