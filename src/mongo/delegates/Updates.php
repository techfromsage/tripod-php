<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\ReadPreference;
use MongoDB\Operation\FindOneAndUpdate;
use MongoDB\UpdateResult;
use Tripod\ChangeSet;
use Tripod\Exceptions\CardinalityException;
use Tripod\Exceptions\Exception;
use Tripod\ExtendedGraph;
use Tripod\IEventHook;
use Tripod\Mongo\Composites\IComposite;
use Tripod\Mongo\Jobs\DiscoverImpactedSubjects;
use Tripod\Timer;

class Updates extends DriverBase
{
    /**
     * constants for the supported hook functions that can be applied.
     */
    public const HOOK_FN_PRE = 'pre';
    public const HOOK_FN_SUCCESS = 'success';
    public const HOOK_FN_FAILURE = 'failure';

    protected Driver $tripod;

    protected ?Database $locksDb = null;

    protected ?Collection $locksCollection = null;

    protected ?DiscoverImpactedSubjects $discoverImpactedSubjects = null;

    private ?TransactionLog $transactionLog = null;

    /**
     * @var string the original read preference gets stored here when changing for a write
     */
    private string $originalCollectionReadPreference = '';

    /**
     * @var string the original read preference gets stored here when changing for a write
     */
    private string $originalDbReadPreference = '';

    private int $retriesToGetLock;

    /**
     * @var array<string, bool> keyed by OP_VIEWS/OP_TABLES/OP_SEARCH; OP_SEARCH is absent when no search provider is configured
     */
    private array $async;

    private ?string $queueName = null;

    /**
     * @var IEventHook[]
     */
    private array $saveChangesHooks = [];

    public function __construct(Driver $tripod, array $opts = [])
    {
        $this->tripod = $tripod;
        $this->storeName = $tripod->getStoreName();
        $this->podName = $tripod->getPodName();
        $this->stat = $tripod->getStat();
        $this->labeller = new Labeller();
        $opts = array_merge([
            'defaultContext' => null,
            OP_ASYNC => [OP_VIEWS => false, OP_TABLES => true, OP_SEARCH => true],
            'stat' => null,
            'readPreference' => ReadPreference::PRIMARY_PREFERRED,
            'retriesToGetLock' => 20,
        ], $opts);
        $this->readPreference = is_string($opts['readPreference']) && $opts['readPreference'] !== ''
            ? $opts['readPreference']
            : ReadPreference::PRIMARY_PREFERRED;
        $this->config = $this->getConfigInstance();

        // default context
        $this->defaultContext = is_string($opts['defaultContext']) ? $opts['defaultContext'] : null;

        // max retries to get lock
        $this->retriesToGetLock = is_int($opts['retriesToGetLock']) ? $opts['retriesToGetLock'] : 20;

        // fill in and default any missing keys for $async array
        $async = is_array($opts[OP_ASYNC]) ? $opts[OP_ASYNC] : [];
        if (!array_key_exists(OP_VIEWS, $async)) {
            $async[OP_VIEWS] = false;
        }

        if (!array_key_exists(OP_TABLES, $async)) {
            $async[OP_TABLES] = true;
        }

        if (!array_key_exists(OP_SEARCH, $async)) {
            $async[OP_SEARCH] = true;
        }

        // if there is no es configured then remove OP_SEARCH from async (no point putting these onto the queue) TRI-19
        if ($this->config->getSearchDocumentSpecifications($this->storeName) == null) {
            unset($async[OP_SEARCH]);
        }

        // If a custom queue name was specified, store it
        if (array_key_exists(OP_QUEUE, $async)) {
            $this->queueName = is_string($async[OP_QUEUE]) ? $async[OP_QUEUE] : null;
            unset($async[OP_QUEUE]);
        }

        $asyncFlags = [];
        foreach ($async as $op => $isAsync) {
            if (is_string($op)) {
                $asyncFlags[$op] = (bool) $isAsync;
            }
        }

        $this->async = $asyncFlags;

        if (isset($opts['statsConfig']) && is_array($opts['statsConfig'])) {
            $this->statsConfig = $opts['statsConfig'];
        }
    }

    /**
     * Create and apply a changeset which is the delta between $oldGraph and $newGraph.
     *
     * @throws \Exception
     */
    public function saveChanges(
        ExtendedGraph $oldGraph,
        ExtendedGraph $newGraph,
        ?string $context = null,
        ?string $description = null
    ): bool {
        $this->applyHooks($this::HOOK_FN_PRE, $this->saveChangesHooks, [
            'pod' => $this->getPodName(),
            'oldGraph' => $oldGraph,
            'newGraph' => $newGraph,
            'context' => $context,
        ]);

        $this->setReadPreferenceToPrimary();

        try {
            $contextAlias = $this->getContextAlias($context);

            if (!$this->getConfigInstance()->isPodWithinStore($this->getStoreName(), $this->getPodName())) {
                throw new Exception('database:collection ' . $this->getStoreName() . ':' . $this->getPodName() . ' is not referenced within config, so cannot be written to');
            }

            $this->validateGraphCardinality($newGraph);

            $oldIndex = $oldGraph->get_index();
            $newIndex = $newGraph->get_index();
            $args = ['before' => $oldIndex, 'after' => $newIndex, 'changeReason' => $description];
            $cs = new ChangeSet($args);

            $subjectsAndPredicatesOfChange = [];
            $transaction_id = null;
            if ($cs->has_changes()) {
                // store the actual CBDs
                $result = $this->storeChanges($cs, $contextAlias);
                if (!isset($result['subjectsAndPredicatesOfChange']) || !isset($result['transaction_id'])) {
                    $this->errorLog('Result of storeChanges malformed, should have transaction_id and subjectsAndPredicatesOfChange array keys', ['result' => $result]);

                    throw new Exception('Result of storeChanges malformed, should have transaction_id and subjectsAndPredicatesOfChange array keys');
                }

                extract($result); // will unpack into $subjectsAndPredicatesOfChange

                // Process any syncronous operations
                $this->processSyncOperations($subjectsAndPredicatesOfChange, $contextAlias);

                // Schedule calculation of any async activity
                $this->queueAsyncOperations($subjectsAndPredicatesOfChange, $contextAlias);
            }

            $this->applyHooks($this::HOOK_FN_SUCCESS, $this->saveChangesHooks, [
                'pod' => $this->getPodName(),
                'oldGraph' => $oldGraph,
                'newGraph' => $newGraph,
                'context' => $context,
                'changeSet' => $cs,
                'subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange,
                'transaction_id' => $transaction_id,
            ]);
        } catch (\Exception $e) {
            // ensure we reset the original read preference in the event of an exception
            $this->resetOriginalReadPreference();
            $this->applyHooks($this::HOOK_FN_FAILURE, $this->saveChangesHooks, [
                'pod' => $this->getPodName(),
                'oldGraph' => $oldGraph,
                'newGraph' => $newGraph,
                'context' => $context,
            ]);

            throw $e;
        }

        $this->resetOriginalReadPreference();

        return true;
    }

    // ////// LOCKS \\\\\\\\
    /**
     * Get locked documents for a date range or all documents if no date range is given.
     */
    public function getLockedDocuments(?string $fromDateTime = null, ?string $tillDateTime = null): array
    {
        $query = [];
        if (!empty($fromDateTime) || !empty($tillDateTime)) {
            $query[_LOCKED_FOR_TRANS_TS] = [];

            if (!empty($fromDateTime)) {
                $query[_LOCKED_FOR_TRANS_TS][MONGO_OPERATION_GTE] = DateUtil::getMongoDate(strtotime($fromDateTime) * 1000);
            }

            if (!empty($tillDateTime)) {
                $query[_LOCKED_FOR_TRANS_TS][MONGO_OPERATION_LTE] = DateUtil::getMongoDate(strtotime($tillDateTime) * 1000);
            }
        }

        $docs = $this->getLocksCollection()->find($query, ['sort' => [_LOCKED_FOR_TRANS => 1]]);

        if ($this->getLocksCollection()->count($query) == 0) {
            return [];
        }

        $res = [];
        foreach ($docs as $doc) {
            $res[] = $doc;
        }

        return $res;
    }

    /**
     * Remove locks that are there forever, creates a audit entry to keep track who and why removed these locks.
     *
     * @throws \Exception If something goes wrong when unlocking documents, or creating audit entries
     */
    public function removeInertLocks(string $transaction_id, string $reason): bool
    {
        $query = [_LOCKED_FOR_TRANS => $transaction_id];
        $docs = $this->getLocksCollection()->find($query);

        if ($this->getLocksCollection()->count($query) == 0) {
            return false;
        }

        // 1. Create audit entry with in_progress status
        $auditCollection = $this->getAuditManualRollbacksCollection();
        $auditDocumentId = $this->generateIdForNewMongoDocument();

        try {
            $documents = [];
            foreach ($docs as $doc) {
                $documents[] = $doc[_ID_KEY][_ID_RESOURCE];
            }

            $result = $auditCollection->insertOne(
                [
                    _ID_KEY => $auditDocumentId,
                    _ID_TYPE => AUDIT_TYPE_REMOVE_INERT_LOCKS,
                    'status' => AUDIT_STATUS_IN_PROGRESS,
                    'reason' => $reason,
                    'transaction_id' => $transaction_id,
                    'documents' => $documents,
                    _CREATED_TS => $this->getMongoDate(),
                ]
            );
            if (!$result->isAcknowledged()) {
                throw new \Exception('Failed to create audit entry: write not acknowledged');
            }
        } catch (\Exception $e) { // simply send false as status as we are unable to create audit entry
            $this->errorLog(
                MONGO_LOCK,
                [
                    'description' => 'Driver::removeInertLocks - failed',
                    'transaction_id' => $transaction_id,
                    'exception-message' => $e->getMessage(),
                ]
            );

            throw $e;
        }

        // we can not try to combine this try-catch with try-catch above.
        // Catch below is supposed to update audit entry with error status but in above catch error can occur when creating audit entry.
        try {
            // 2. Unlock documents linked to transaction
            $this->unlockAllDocuments($transaction_id);

            // 3. Update audit entry to say it was completed

            $result = $auditCollection->updateOne([_ID_KEY => $auditDocumentId], [MONGO_OPERATION_SET => ['status' => AUDIT_STATUS_COMPLETED, _UPDATED_TS => $this->getMongoDate()]]);

            if (!$result->isAcknowledged()) {
                throw new \Exception('Failed to update audit entry: write not acknowledged');
            }
        } catch (\Exception $e) {
            $logInfo = [
                'description' => 'Driver::removeInertLocks - failed',
                'transaction_id' => $transaction_id,
                'exception-message' => $e->getMessage(),
            ];

            // 4. Update audit entry to say it was failed with error
            $result = $auditCollection->updateOne([_ID_KEY => $auditDocumentId], [MONGO_OPERATION_SET => ['status' => AUDIT_STATUS_ERROR, _UPDATED_TS => $this->getMongoDate(), 'error' => $e->getMessage()]]);

            if (!$result->isAcknowledged()) {
                $logInfo['additional-error'] = 'Failed to update audit entry: write not acknowledged';
            }

            $this->errorLog(MONGO_LOCK, $logInfo);

            throw $e;
        }

        return true;
    }

    // /////// REPLAY TRANSACTION LOG ///////
    /**
     * replays all transactions from the transaction log, use the function params to control the from and to date if you
     * only want to replay transactions created during specific window.
     *
     * @param string|null $fromDate only transactions after this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate   only transactions before this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     */
    public function replayTransactionLog(?string $fromDate = null, ?string $toDate = null): bool
    {
        $cursor = $this->getTransactionLog()->getCompletedTransactions($this->storeName, $this->podName, $fromDate, $toDate);
        foreach ($cursor as $result) {
            $this->applyTransaction($result);
        }

        return true;
    }

    // getters and setters for the delegates
    public function getTransactionLog(): TransactionLog
    {
        if ($this->transactionLog == null) {
            $this->transactionLog = new TransactionLog();
        }

        return $this->transactionLog;
    }

    public function setTransactionLog(TransactionLog $transactionLog): void
    {
        $this->transactionLog = $transactionLog;
    }

    /**
     * Register save changes event hooks.
     */
    public function registerSaveChangesEventHook(IEventHook $hook): void
    {
        $this->saveChangesHooks[] = $hook;
    }

    /**
     * @param IEventHook[] $hooks
     *
     * @throws Exception If an invalid hook function is requested
     */
    protected function applyHooks(string $fn, array $hooks, array $args = []): void
    {
        switch ($fn) {
            case $this::HOOK_FN_PRE:
            case $this::HOOK_FN_SUCCESS:
            case $this::HOOK_FN_FAILURE:
                break;

            default:
                throw new Exception(sprintf('Invalid hook function %s requested', $fn));
        }

        foreach ($hooks as $hook) {
            try {
                $hookFn = [$hook, $fn];
                if (is_callable($hookFn)) {
                    call_user_func($hookFn, $args);
                }
            } catch (\Exception $e) {
                // don't let rabid hooks stop tripod
                static::getLogger()->error('Hook ' . get_class($hook) . sprintf(' threw exception %s, continuing', $e->getMessage()));
            }
        }
    }

    /**
     * Change the read preferences to RP_PRIMARY
     * Used for a write operation.
     */
    protected function setReadPreferenceToPrimary(): void
    {
        // Set db preference
        /** @var ReadPreference $dbReadPref */
        $dbReadPref = $this->getDatabase()->getReadPreference();

        $dbPref = $dbReadPref->getModeString();
        $dbTagsets = $dbReadPref->getTagsets();

        $this->originalDbReadPreference = $dbPref;
        if ($dbPref !== ReadPreference::PRIMARY) {
            $this->db = $this->getDatabase()->withOptions(['readPreference' => new ReadPreference(ReadPreference::PRIMARY, $dbTagsets)]);
        }

        /** @var ReadPreference $collReadPref */
        $collReadPref = $this->getCollection()->getReadPreference();
        $collPref = $collReadPref->getModeString();
        $collTagsets = $collReadPref->getTagsets();

        // Set collection preference
        $this->originalCollectionReadPreference = $collPref;
        if ($collPref !== ReadPreference::PRIMARY) {
            $this->collection = $this->getCollection()->withOptions(['readPreference' => new ReadPreference(ReadPreference::PRIMARY, $collTagsets)]);
        }
    }

    /**
     * Reset the original read preference after changing with setReadPreferenceToPrimary.
     */
    protected function resetOriginalReadPreference(): void
    {
        $dbReadPref = $this->getDatabase()->getReadPreference();
        if ($this->originalDbReadPreference !== $dbReadPref->getModeString()) {
            $pref = $this->originalDbReadPreference ?: $this->readPreference;
            $dbTagsets = $dbReadPref->getTagsets();

            $this->db = $this->getDatabase()->withOptions([
                'readPreference' => new ReadPreference($pref, $dbTagsets),
            ]);
        }

        // Reset collection object
        $collReadPref = $this->getCollection()->getReadPreference();
        if ($this->originalCollectionReadPreference !== $collReadPref->getModeString()) {
            $pref = $this->originalCollectionReadPreference ?: $this->readPreference;
            $collTagsets = $collReadPref->getTagsets();
            $this->collection = $this->getCollection()->withOptions([
                'readPreference' => new ReadPreference($pref, $collTagsets),
            ]);
        }
    }

    /**
     * Ensure that the graph we want to persist has data with valid cardinality.
     *
     * @throws CardinalityException
     */
    protected function validateGraphCardinality(ExtendedGraph $graph): void
    {
        $config = $this->getConfigInstance();
        $cardinality = $config->getCardinality($this->getStoreName(), $this->getPodName());
        $namespaces = $config->getNamespaces();
        $graphSubjects = $graph->get_subjects();

        if (empty($cardinality) || $graph->is_empty()) {
            return;
        }

        foreach ($cardinality as $qname => $cardinalityValue) {
            [$namespace, $predicateName] = explode(':', $qname);
            if (!array_key_exists($namespace, $namespaces)) {
                throw new CardinalityException(sprintf("Namespace '%s' not defined for qname: %s", $namespace, $qname));
            }

            // NB: The only constraint we currently support is a value of 1 to enforce one triple per subject/predicate.
            if ($cardinalityValue == 1) {
                foreach ($graphSubjects as $subjectUri) {
                    $predicateUri = $namespaces[$namespace] . $predicateName;
                    $predicateValues = $graph->get_subject_property_values($subjectUri, $predicateUri);
                    if (count($predicateValues) > 1) {
                        $v = [];
                        foreach ($predicateValues as $predicateValue) {
                            $v[] = $predicateValue['value'];
                        }

                        throw new CardinalityException(sprintf("Cardinality failed on %s for '%s' - should only have 1 value and has: ", $subjectUri, $qname) . implode(', ', $v));
                    }
                }
            }
        }
    }

    /**
     * @param ChangeSet $cs Change-set to apply
     *
     * @return array An array of subjects and predicates that have been changed
     *
     * @throws Exception
     */
    protected function storeChanges(ChangeSet $cs, string $contextAlias): array
    {
        $t = new Timer();
        $t->start();

        $subjectsOfChange = $cs->get_subjects_of_change();
        $transaction_id = $this->generateTransactionId();

        // store the details of the transaction in the transaction log
        $mongoGraph = new MongoGraph();
        $mongoGraph->_index = $cs->_index;

        $csDoc = $mongoGraph->to_tripod_view_array('changes', $contextAlias); // todo - this changed to tripod view array, why is "changes" the docId?
        $originalCBDs = [];

        // apply the changes
        try {
            // 1. lock all documents
            // 2. create new transaction
            // 3. apply changes
            // 4. unlock all documents
            // 5. complete transaction

            $originalCBDs = $this->lockAllDocuments($subjectsOfChange, $transaction_id, $contextAlias);

            $this->getTransactionLog()->createNewTransaction($transaction_id, $csDoc['value'][_GRAPHS], $originalCBDs, $this->getStoreName(), $this->getPodName());

            if (empty($originalCBDs)) { // didn't get lock on documents
                $this->getTransactionLog()->failTransaction($transaction_id, new \Exception('Did not obtain locks on documents'));

                throw new \Exception('Did not obtain locks on documents');
            }

            $changes = $this->applyChangeSet($cs, $originalCBDs, $contextAlias, $transaction_id);

            $this->debugLog(
                MONGO_LOCK,
                [
                    'description' => 'Driver::storeChanges - Unlocking documents, apply change-set completed',
                    'transaction_id' => $transaction_id,
                ]
            );

            $this->unlockAllDocuments($transaction_id);
            $this->getTransactionLog()->completeTransaction($transaction_id, $changes['newCBDs']);

            $t->stop();
            $this->timingLog(MONGO_WRITE, ['duration' => $t->result(), 'subjectsOfChange' => implode(', ', $subjectsOfChange)]);
            $this->getStat()->timer(MONGO_WRITE . ('.' . $this->getPodName()), $t->result());

            return $changes;
        } catch (\Exception $e) {
            $this->getStat()->increment(MONGO_ROLLBACK);
            $this->errorLog(
                MONGO_ROLLBACK,
                [
                    'description' => 'Save Failed Rolling back transaction:' . $e->getMessage(),
                    'transaction_id' => $transaction_id,
                    'subjectsOfChange' => implode(',', $subjectsOfChange),
                    'exceptionMessage' => $e->getMessage(),
                ]
            );
            $this->rollbackTransaction($transaction_id, $originalCBDs, $e);

            throw new Exception('Error storing changes: ' . $e->getMessage() . ' >>>' . $e->getTraceAsString(), $e->getCode(), $e);
        }
    }

    /**
     * @param string     $transaction_id id of the transaction
     * @param array|null $originalCBDs   containing the original CBDS
     *
     * @throws \Throwable
     */
    protected function rollbackTransaction(string $transaction_id, ?array $originalCBDs, \Throwable $exception): bool
    {
        // set transaction to cancelling
        $this->getTransactionLog()->cancelTransaction($transaction_id, $exception);

        if (!empty($originalCBDs)) {  // restore the original CBDs
            foreach ($originalCBDs as $g) {
                /** @var UpdateResult $result */
                $result = $this->updateCollection([_ID_KEY => $g[_ID_KEY]], $g, ['w' => 1]);

                if (!$result->isAcknowledged()) {
                    // Error log here
                    $this->errorLog(
                        MONGO_ROLLBACK,
                        [
                            'description' => 'Driver::rollbackTransaction - Error updating transaction',
                            'exception_message' => $exception->getMessage(),
                            'transaction_id' => $transaction_id,
                        ]
                    );

                    throw new \Exception(sprintf('Failed to restore Original CBDS for transaction: %s stopped at ', $transaction_id) . $g[_ID_KEY]);
                }
            }
        } else {
            $this->errorLog(
                MONGO_ROLLBACK,
                [
                    'description' => 'Driver::rollbackTransaction - Unlocking documents',
                    'exception_message' => $exception->getMessage(),
                    'transaction_id' => $transaction_id,
                ]
            );
        }

        $this->unlockAllDocuments($transaction_id);

        // set transaction to failed
        $this->getTransactionLog()->failTransaction($transaction_id);

        return true;
    }

    /**
     * Returns a unique transaction ID.
     */
    protected function generateTransactionId(): string
    {
        return 'transaction_' . $this->getUniqId();
    }

    /**
     * Returns a unique id: for mocking.
     */
    protected function getUniqId(): string
    {
        return uniqid('', true);
    }

    /**
     * Adds/updates/deletes the graph in the database.
     *
     * @return array{newCBDs: list<array>, subjectsAndPredicatesOfChange: array<string, list<string>>, transaction_id: string}
     *
     * @throws \Exception
     */
    protected function applyChangeSet(ChangeSet $cs, array $originalCBDs, string $contextAlias, string $transaction_id): array
    {
        $subjectsAndPredicatesOfChange = [];
        if (in_array($this->getCollection()->getCollectionName(), $this->getConfigInstance()->getPods($this->getStoreName()))) {
            // how many subjects of change?
            $changes = $cs->get_subjects_of_type($this->labeller->qname_to_uri('cs:ChangeSet'));

            // gather together all the updates (we'll apply them later)....
            $updates = [];

            $newCBDs = [];

            foreach ($changes as $changeUri) {
                $subjectOfChange = $cs->get_first_resource($changeUri, $this->labeller->qname_to_uri('cs:subjectOfChange'));
                if ($subjectOfChange === null) {
                    throw new Exception('ChangeSet ' . $changeUri . ' has no cs:subjectOfChange');
                }
                if (!array_key_exists($subjectOfChange, $subjectsAndPredicatesOfChange)) {
                    $subjectsAndPredicatesOfChange[$subjectOfChange] = [];
                }

                $criteria = [
                    _ID_KEY => [_ID_RESOURCE => $this->labeller->uri_to_alias($subjectOfChange), _ID_CONTEXT => $contextAlias],
                ];

                // read before write, and to find array indexes and get document in memory

                $doc = $this->getDocumentForUpdate($subjectOfChange, $contextAlias, $originalCBDs);

                $upsert = false;
                if (empty($doc)) {
                    $upsert = true;
                }

                // add the old vals to critera
                $changesGroupedByNsPredicate = $this->getAdditionsRemovalsGroupedByNsPredicate($cs, $changeUri);

                $mongoUpdateOperations = [];
                foreach ($changesGroupedByNsPredicate as $nsPredicate => $additionsRemovals) {
                    $predicateExists = isset($doc[$nsPredicate]);

                    $predicate = $this->labeller->qname_to_uri($nsPredicate);
                    if (!in_array($predicate, $subjectsAndPredicatesOfChange[$subjectOfChange])) {
                        $subjectsAndPredicatesOfChange[$subjectOfChange][] = $predicate;
                    }

                    // set to existing object if exists
                    $valueObject = ($predicateExists) ? $doc[$nsPredicate] : [];
                    if (isset($valueObject[VALUE_URI]) || isset($valueObject[VALUE_LITERAL])) {
                        // this is a single value object, convert to array of values for now
                        $valueObject = [$valueObject];
                    }

                    if (isset($additionsRemovals['additions'])) {
                        foreach ($additionsRemovals['additions'] as $addition) {
                            $valueObject[] = $addition;
                        }
                    }

                    // remove
                    if (isset($additionsRemovals['removals'])) {
                        $elemsToRemove = [];
                        foreach ($additionsRemovals['removals'] as $removal) {
                            $valueIndex = array_search($removal, $valueObject, true);
                            if ($valueIndex === false) {
                                $values = array_values($removal);
                                $v = array_pop($values);
                                $this->errorLog(sprintf('Removal value %s %s %s does not appear in target document to be updated', $subjectOfChange, $predicate, $v), ['doc' => $doc]);

                                throw new \Exception(sprintf('Removal value %s %s %s does not appear in target document to be updated', $subjectOfChange, $predicate, $v));
                            }

                            $elemsToRemove[] = $valueIndex;
                        }

                        if ($elemsToRemove !== []) {
                            foreach ($elemsToRemove as $elem) {
                                unset($valueObject[$elem]);
                            }

                            $valueObject = array_values($valueObject); // renumbers array after unsets
                        }
                    }

                    if (count($valueObject) > 0) {
                        // unique value object
                        $valueObject = array_map('unserialize', array_unique(array_map('serialize', $valueObject)));
                        if (count($valueObject) === 1) {
                            $valueObject = $valueObject[0]; // un-array if only one value
                        }

                        $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_SET, [$nsPredicate => $valueObject]);
                    } elseif ($predicateExists) {
                        // remove all existing values, if existed in the first place
                        $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_UNSET, [$nsPredicate => 1]);
                    }
                }

                // todo: criteria at this point should probably include all removal statements if they exist
                // i.e. we only want to update document if it has all these values ( think platform 409 )
                // currently the only criteria is the doc id
                $updatedAt = DateUtil::getMongoDate();

                if (!isset($doc[_VERSION])) {
                    // new doc
                    $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_SET, [_VERSION => 0]);
                    $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_SET, [_CREATED_TS => $updatedAt]);
                } else {
                    $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_INC, [_VERSION => 1]);
                }

                $this->addOperatorToChange($mongoUpdateOperations, MONGO_OPERATION_SET, [_UPDATED_TS => $updatedAt]);

                $updates[] = ['criteria' => $criteria, 'changes' => $mongoUpdateOperations, 'upsert' => $upsert];
            }

            // apply each update
            foreach ($updates as $update) {
                try {
                    $newDoc = $this->getCollection()->findOneAndUpdate($update['criteria'], $update['changes'], [
                        'upsert' => $update['upsert'],
                        'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER,
                    ]);
                    if ($newDoc) {
                        $newCBDs[] = $newDoc;
                    }
                } catch (\Exception $e) {
                    $this->errorLog(
                        MONGO_WRITE,
                        [
                            'description' => 'Error with Mongo DB findOneAndUpdate:' . $e->getMessage(),
                            'transaction_id' => $transaction_id,
                        ]
                    );

                    throw $e;
                }
            }

            return [
                'newCBDs' => $newCBDs,
                'subjectsAndPredicatesOfChange' => $this->subjectsAndPredicatesOfChangeUrisToAliases($subjectsAndPredicatesOfChange),
                'transaction_id' => $transaction_id,
            ];
        }

        throw new \Exception('Attempted to update a non-CBD collection');
    }

    /**
     * Normalize our subjects and predicates of change to use aliases rather than fq uris.
     *
     * @param array<string, list<string>> $subjectsAndPredicatesOfChange
     *
     * @return array<string, list<string>>
     */
    protected function subjectsAndPredicatesOfChangeUrisToAliases(array $subjectsAndPredicatesOfChange): array
    {
        $aliases = [];
        foreach ($subjectsAndPredicatesOfChange as $subject => $predicates) {
            $subjectAlias = $this->labeller->uri_to_alias($subject);
            $aliases[$subjectAlias] = [];
            foreach ($predicates as $predicate) {
                $aliases[$subjectAlias][] = $this->labeller->uri_to_alias($predicate);
            }
        }

        return $aliases;
    }

    /**
     * Given a set of CBD's return the CBD that matches the Subject of Change.
     *
     * @return array|null the document from the collection of $cbds that matches the subject of change
     */
    protected function getDocumentForUpdate(string $subjectOfChange, string $contextAlias, array $cbds): ?array
    {
        foreach ($cbds as $c) {
            if (is_array($c) && $c[_ID_KEY] == [_ID_RESOURCE => $this->labeller->uri_to_alias($subjectOfChange), _ID_CONTEXT => $contextAlias]) {
                return $c;
            }
        }

        return null;
    }

    /**
     * Processes each subject synchronously.
     */
    protected function processSyncOperations(array $subjectsAndPredicatesOfChange, string $contextAlias): void
    {
        foreach ($this->getSyncOperations() as $op) {
            /** @var IComposite $composite */
            $composite = $this->tripod->getComposite($op);
            $opSubjects = $composite->getImpactedSubjects($subjectsAndPredicatesOfChange, $contextAlias);
            if (!empty($opSubjects)) {
                foreach ($opSubjects as $subject) {
                    $t = new Timer();
                    $t->start();

                    // Call update on the subject, rather than the composite directly, in case the change was to
                    // another pod
                    $subject->update();

                    $t->stop();

                    $this->timingLog(MONGO_ON_THE_FLY_MR, [
                        'duration' => $t->result(),
                        'storeName' => $subject->getStoreName(),
                        'podName' => $subject->getPodName(),
                        'resourceId' => $subject->getResourceId(),
                    ]);
                    $this->getStat()->timer(MONGO_ON_THE_FLY_MR, $t->result());
                }
            }
        }
    }

    // ///////////////////////// QUEUE RELATED METHODS BELOW HERE ///////////////////////////////////////

    /**
     * Adds the operations to the queue to be performed asynchronously.
     */
    protected function queueASyncOperations(array $subjectsAndPredicatesOfChange, string $contextAlias): void
    {
        $operations = $this->getAsyncOperations();
        if ($operations !== []) {
            $data = [
                'changes' => $subjectsAndPredicatesOfChange,
                'operations' => $operations,
                'storeName' => $this->storeName,
                'podName' => $this->podName,
                'contextAlias' => $contextAlias,
                'statsConfig' => $this->getStatsConfig(),
            ];

            if ($this->queueName !== null) {
                $data[OP_QUEUE] = $this->queueName;
                $queueName = $this->queueName;
            } else {
                $configInstance = $this->getConfigInstance();
                $queueName = $configInstance::getDiscoverQueueName();
            }

            $this->getDiscoverImpactedSubjects()->createJob($data, $queueName);
        }
    }

    /**
     * For mocking.
     */
    protected function getDiscoverImpactedSubjects(): DiscoverImpactedSubjects
    {
        if ($this->discoverImpactedSubjects === null) {
            $this->discoverImpactedSubjects = new DiscoverImpactedSubjects();
        }

        return $this->discoverImpactedSubjects;
    }

    /**
     * Attempts to lock all subjects of change in a pass, if failed unlocked locked subjects and do a retry of all again.
     *
     * @param array    $subjectsOfChange array of the subjects that are part of this transaction
     * @param string   $transaction_id   id for this transaction
     * @param string[] $subjectsOfChange
     *
     * @return array|null returns an array of CBDs, each CBD is the version at the time at which the lock was attained
     *
     * @throws \Exception
     */
    protected function lockAllDocuments(array $subjectsOfChange, string $transaction_id, string $contextAlias): ?array
    {
        for ($retry = 1; $retry <= $this->retriesToGetLock; $retry++) {
            $originalCBDs = [];
            $lockedSubjects = [];
            foreach ($subjectsOfChange as $s) {
                $this->debugLog(
                    MONGO_LOCK,
                    [
                        'description' => 'Driver::lockAllDocuments - Attempting to get lock',
                        'transaction_id' => $transaction_id,
                        'subject' => $s,
                        'attempt' => $retry,
                    ]
                );

                $document = $this->lockSingleDocument($s, $transaction_id, $contextAlias);
                if (!empty($document)) {
                    $this->debugLog(
                        MONGO_LOCK,
                        [
                            'description' => 'Driver::lockAllDocuments - Got the lock',
                            'transaction_id' => $transaction_id,
                            'subject' => $s,
                            'retry' => $retry,
                        ]
                    );

                    $this->getStat()->increment(MONGO_LOCK);
                    $originalCBDs[] = $document;
                    $lockedSubjects[] = $s;
                }
            }

            if (count($subjectsOfChange) === count($lockedSubjects)) {
                // if all subjects of change locked, we are good.
                return $originalCBDs;
            }

            // If any subject was locked, unlock it
            if ($lockedSubjects !== []) {
                $this->unlockAllDocuments($transaction_id);
            }

            $this->debugLog(
                MONGO_LOCK,
                [
                    'description' => 'Driver::lockAllDocuments - Unable to lock all ' . count($subjectsOfChange) . '  documents, unlocked  ' . count($lockedSubjects) . ' locked documents',
                    'transaction_id' => $transaction_id,
                    'documentsToLock' => implode(',', $subjectsOfChange),
                    'documentsLocked' => implode(',', $lockedSubjects),
                    'retry' => $retry,
                ]
            );
            $n = mt_rand(25, 40);
            usleep($n * 1000); // do a retry
        }

        $this->errorLog(
            MONGO_LOCK,
            [
                'description' => 'Unable to lock all required documents. Exhausted retries',
                'retries' => $this->retriesToGetLock,
                'transaction_id' => $transaction_id,
                'subjectsOfChange' => implode(', ', $subjectsOfChange),
            ]
        );

        return null;
    }

    /**
     * Unlocks documents locked by current transaction.
     *
     * @param string $transaction_id id for this transaction
     *
     * @throws \Exception is thrown if for any reason the update to mongo fails
     */
    protected function unlockAllDocuments(string $transaction_id): bool
    {
        $result = $this->getLocksCollection()->deleteMany([_LOCKED_FOR_TRANS => $transaction_id], ['w' => 1]);

        if (!$result->isAcknowledged()) {
            $this->errorLog(
                MONGO_LOCK,
                [
                    'description' => 'Driver::unlockAllDocuments - Failed to unlock documents (transaction_id - ' . $transaction_id . ')',
                    $result,
                ]
            );

            throw new \Exception('Failed to unlock documents as part of transaction : ' . $transaction_id);
        }

        return true;
    }

    /**
     * Lock and return a single document for editing.
     *
     * @param string $s subject URI of resource to lock
     *
     * @return array|false|null
     */
    protected function lockSingleDocument(string $s, string $transaction_id, string $contextAlias)
    {
        $countEntriesInLocksCollection = $this->getLocksCollection()
            ->count(
                [
                    _ID_KEY => [
                        _ID_RESOURCE => $this->labeller->uri_to_alias($s),
                        _ID_CONTEXT => $contextAlias,
                    ],
                ]
            );

        if ($countEntriesInLocksCollection > 0) { // Subject is already locked
            return false;
        }

        try { // Add a entry to locks collection for this subject, will throws exception if an entry already there
            $result = $this->getLocksCollection()->insertOne(
                [
                    _ID_KEY => [_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias],
                    _LOCKED_FOR_TRANS => $transaction_id,
                    _LOCKED_FOR_TRANS_TS => DateUtil::getMongoDate(),
                ],
                ['w' => 1]
            );

            if (!$result->isAcknowledged()) {
                throw new \Exception('Failed to lock document: write not acknowledged');
            }
        } catch (\Exception $e) { // Subject is already locked or unable to lock
            $this->debugLog(
                MONGO_LOCK,
                [
                    'description' => 'Driver::lockSingleDocument - failed with exception',
                    'transaction_id' => $transaction_id,
                    'subject' => $s,
                    'exception-message' => $e->getMessage(),
                ]
            );

            return false;
        }

        // Let's get original document for processing.
        $document = $this->getCollection()->findOne([_ID_KEY => [_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias]]);
        if (empty($document)) { // if document is not there, create it
            try {
                $result = $this->getCollection()->insertOne(
                    [
                        _ID_KEY => [_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias],
                    ],
                    ['w' => 1]
                );

                if (!$result->isAcknowledged()) {
                    throw new \Exception('Failed to create new document: write not acknowledged');
                }

                $document = $this->getCollection()->findOne([_ID_KEY => [_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias]]);
            } catch (\Exception $e) {
                $this->errorLog(
                    MONGO_LOCK,
                    [
                        'description' => 'Driver::lockSingleDocument - failed when creating new document',
                        'transaction_id' => $transaction_id,
                        'subject' => $s,
                        'exception-message' => $e->getMessage(),
                    ]
                );

                return false;
            }
        }

        return $document;
    }

    // / Collection methods
    protected function getAuditManualRollbacksCollection(): Collection
    {
        return $this->config->getCollectionForManualRollbackAudit($this->storeName);
    }

    protected function generateIdForNewMongoDocument(): ObjectId
    {
        return new ObjectId();
    }

    protected function getMongoDate(): UTCDateTime
    {
        return DateUtil::getMongoDate();
    }

    /**
     * Saves a transaction.
     */
    protected function applyTransaction(array $transaction): void
    {
        $changes = $transaction['changes'];
        $newCBDs = $transaction['newCBDs'];

        $subjectsOfChange = [];
        foreach ($changes as $c) {
            if ($c['rdf:type'][VALUE_URI] == 'cs:ChangeSet') {
                $subjectsOfChange[] = $c['cs:subjectOfChange'][VALUE_URI];
            }
        }

        foreach ($subjectsOfChange as $s) {
            foreach ($newCBDs as $n) {
                if ($n[_ID_KEY][_ID_RESOURCE] == $s) {
                    $this->updateCollection([_ID_KEY => $n[_ID_KEY]], $n, ['upsert' => true]);

                    break;
                }
            }
        }
    }

    /**
     * Creates a new Driver instance.
     */
    protected function getTripod(array $data): Driver
    {
        return new Driver(
            $data['collection'],
            $data['database'],
            ['stat' => $this->stat]
        );
    }

    /**
     * This proxy method allows us to mock updates against $this->collection.
     *
     * @param array|object $query
     * @param array|object $update
     */
    protected function updateCollection($query, $update, array $options): UpdateResult
    {
        return $this->getCollection()->replaceOne($query, $update, $options);
    }

    protected function getLocksDatabase(): Database
    {
        if ($this->locksDb === null) {
            $this->locksDb = $this->config->getDatabase($this->storeName);
        }

        return $this->locksDb;
    }

    protected function getLocksCollection(): Collection
    {
        if ($this->locksCollection === null) {
            $this->locksCollection = $this->getLocksDatabase()->selectCollection(LOCKS_COLLECTION);
        }

        return $this->locksCollection;
    }

    /**
     * Helper function to group the changes for $changeUri by namespaced predicate, then by additions and removals.
     */
    private function getAdditionsRemovalsGroupedByNsPredicate(ChangeSet $cs, string $changeUri): array
    {
        $additionsGroupedByNsPredicate = $this->getChangesGroupedByNsPredicate($cs, $changeUri, $this->labeller->qname_to_uri('cs:addition'));
        $removalsGroupedByNsPredicate = $this->getChangesGroupedByNsPredicate($cs, $changeUri, $this->labeller->qname_to_uri('cs:removal'));

        $mergedResult = [];
        foreach ($additionsGroupedByNsPredicate as $predicate => $values) {
            if (!isset($mergedResult[$predicate])) {
                $mergedResult[$predicate] = [];
            }

            $mergedResult[$predicate]['additions'] = $values;
        }

        foreach ($removalsGroupedByNsPredicate as $predicate => $values) {
            if (!isset($mergedResult[$predicate])) {
                $mergedResult[$predicate] = [];
            }

            $mergedResult[$predicate]['removals'] = $values;
        }

        return $mergedResult;
    }

    /**
     * Helper method to group changes for $changeUri of a given type by namespaced predicate.
     *
     * @param string|string[] $changePredicate
     *
     * @throws Exception
     */
    private function getChangesGroupedByNsPredicate(ChangeSet $cs, string $changeUri, $changePredicate): array
    {
        $changes = $cs->get_subject_property_values($changeUri, $changePredicate);

        $changesGroupedByNsPredicate = [];
        foreach ($changes as $c) {
            $changeNode = (string) $c['value'];
            $predicate = $cs->get_first_resource($changeNode, $this->labeller->qname_to_uri('rdf:predicate'));
            $nsPredicate = (string) $this->labeller->uri_to_qname($predicate);

            if (!array_key_exists($nsPredicate, $changesGroupedByNsPredicate)) {
                $changesGroupedByNsPredicate[$nsPredicate] = [];
            }

            $object = $cs->get_subject_property_values($changeNode, $this->labeller->qname_to_uri('rdf:object'));
            if (count($object) !== 1) {
                $this->getLogger()->error('Expecting object array with exactly 1 element', $object);

                throw new Exception('Object of removal malformed');
            }

            $valueType = ($object[0]['type'] == 'uri') ? VALUE_URI : VALUE_LITERAL;
            $value = ($valueType === VALUE_URI) ? $this->labeller->uri_to_alias((string) $object[0]['value']) : $object[0]['value'];

            $changesGroupedByNsPredicate[$nsPredicate][] = [$valueType => $value];
        }

        return $changesGroupedByNsPredicate;
    }

    /**
     * Helper method to add operator to a set of existing changes ready to be sent to Mongo.
     */
    private function addOperatorToChange(array &$changes, string $operator, array $kvp): void
    {
        if (!isset($changes[$operator]) || !is_array($changes[$operator])) {
            $changes[$operator] = [];
        }

        foreach ($kvp as $key => $value) {
            if (isset($changes[$operator][$key])) {
                if (!is_array($changes[$operator][$key])) {
                    $changes[$operator][$key] = [$changes[$operator][$key]];
                }

                $changes[$operator][$key][] = $value;
            } else {
                $changes[$operator][$key] = $value;
            }
        }
    }

    /**
     * @return string[]
     */
    private function getAsyncOperations(): array
    {
        $types = [];
        foreach ($this->async as $op => $isAsync) {
            if ($isAsync) {
                $types[] = $op;
            }
        }

        return $types;
    }

    /**
     * @return string[]
     */
    private function getSyncOperations(): array
    {
        $types = [];
        foreach ($this->async as $op => $isAsync) {
            if (!$isAsync) {
                $types[] = $op;
            }
        }

        return $types;
    }
}
