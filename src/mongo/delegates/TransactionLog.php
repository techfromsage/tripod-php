<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\CursorInterface;
use MongoDB\InsertOneResult;
use MongoDB\UpdateResult;
use Tripod\Config;

class TransactionLog
{
    protected array $config;

    private Database $transaction_db;

    private Collection $transaction_collection;

    /**
     * Constructor.
     */
    public function __construct()
    {
        $config = Config::getInstance();
        $this->config = $config->getTransactionLogConfig();

        $this->transaction_db = $config->getTransactionLogDatabase();
        $this->transaction_collection = $this->transaction_db->selectCollection($this->config['collection']);
    }

    /**
     * @param string     $transaction_id - the id you wish to assign to the new transaction
     * @param array      $changes        - an array serialization of the changeset to be applied
     * @param array|null $originalCBDs   - an array of the serialized CBDs
     * @param string     $storeName      - the name of the database the changes are being applied to
     * @param string     $podName        - the name of the collection, in the database, the changes are being applied to
     *
     * @throws \Tripod\Exceptions\Exception
     */
    public function createNewTransaction(string $transaction_id, array $changes, ?array $originalCBDs, string $storeName, string $podName): void
    {
        $transaction = [
            _ID_KEY => $transaction_id,
            'dbName' => $storeName,
            'collectionName' => $podName,
            'changes' => $changes,
            'status' => 'in_progress',
            'startTime' => DateUtil::getMongoDate(),
            'originalCBDs' => $originalCBDs,
            'sessionId' => ((session_id() != '') ? session_id() : ''),
        ];

        try {
            $result = $this->insertTransaction($transaction);
            if (!$result->isAcknowledged()) {
                throw new \Exception('Error creating new transaction');
            }
        } catch (\Exception $e) {
            throw new \Tripod\Exceptions\Exception('Error creating new transaction: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Updates the status of a transaction to cancelling.
     * If you passed in an Exception, the exception is logged in the transaction log.
     *
     * @param string     $transaction_id the id of the transaction you wish to cancel
     * @param \Throwable $error          pass in the exception you wish to log
     */
    public function cancelTransaction(string $transaction_id, ?\Throwable $error = null): void
    {
        $params = ['status' => 'cancelling'];
        if ($error != null) {
            $params['error'] = ['reason' => $error->getMessage(), 'trace' => $error->getTraceAsString()];
        }

        $this->updateTransaction(
            [_ID_KEY => $transaction_id],
            ['$set' => $params],
            ['w' => 1, 'upsert' => true]
        );
    }

    /**
     * Updates the status of a transaction to failed, and adds a fail time.
     * If you passed in an Exception, the exception is logged in the transaction log.
     *
     * @param string     $transaction_id the id of the transaction you wish to set as failed
     * @param \Throwable $error          exception you wish to log
     */
    public function failTransaction(string $transaction_id, ?\Throwable $error = null): void
    {
        $params = ['status' => 'failed', 'failedTime' => DateUtil::getMongoDate()];
        if ($error != null) {
            $params['error'] = ['reason' => $error->getMessage(), 'trace' => $error->getTraceAsString()];
        }

        $this->updateTransaction(
            [_ID_KEY => $transaction_id],
            ['$set' => $params],
            ['w' => 1, 'upsert' => true]
        );
    }

    /**
     * Update the status of a transaction to completed, and adds an end time.
     *
     * @param string $transaction_id - the id of the transaction you want to mark as completed
     * @param array  $newCBDs        array of CBD's that represent the after state for each modified entity
     */
    public function completeTransaction(string $transaction_id, array $newCBDs): void
    {
        $this->updateTransaction(
            [_ID_KEY => $transaction_id],
            ['$set' => ['status' => 'completed', 'endTime' => DateUtil::getMongoDate(), 'newCBDs' => $newCBDs]],
            ['w' => 1]
        );
    }

    /**
     * Retrieves a transaction from the transaction based on its id.  The transaction is returned as an array.
     *
     * @param string $transaction_id - the id of the transaction you wish to retrieve from the transaction log
     *
     * @return array|null representing the transaction document
     */
    public function getTransaction(string $transaction_id): ?array
    {
        return $this->transaction_collection->findOne([_ID_KEY => $transaction_id]);
    }

    /**
     * Purges all transactions from the transaction log.
     */
    public function purgeAllTransactions(): void
    {
        $this->transaction_collection->drop();
    }

    /**
     * @param string|null $fromDate only transactions after this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate   only transactions before this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     *
     * @return CursorInterface<int, array<string, mixed>>&\Iterator<int, array<string, mixed>>
     *
     * @throws \InvalidArgumentException
     */
    public function getCompletedTransactions(?string $storeName = null, ?string $podName = null, ?string $fromDate = null, ?string $toDate = null)
    {
        $query = [];
        $query['status'] = 'completed';

        if (!empty($storeName) && !empty($podName)) {
            $query['dbName'] = $storeName;
            $query['collectionName'] = $podName;
        }

        if (!empty($fromDate)) {
            $q = [];
            $q['$gte'] = DateUtil::getMongoDate(strtotime($fromDate) * 1000);

            if (!empty($toDate)) {
                $q['$lte'] = DateUtil::getMongoDate(strtotime($toDate) * 1000);
            }

            $query['endTime'] = $q;
        }

        return $this->transaction_collection->find($query, ['sort' => ['endTime' => 1]]);
    }

    /**
     * @return int Total number of transactions in the transaction log
     */
    public function getTotalTransactionCount(): int
    {
        return $this->transaction_collection->count([]);
    }

    /**
     * @param string|null $storeName database name to filter on (optional)
     * @param string|null $podName   collectionName to filter on (optional)
     *
     * @return int Total number of completed transactions in the transaction log
     *
     * @codeCoverageIgnore
     */
    public function getCompletedTransactionCount(?string $storeName = null, ?string $podName = null): int
    {
        if (!empty($storeName) && !empty($podName)) {
            return $this->transaction_collection->count(['status' => 'completed', 'dbName' => $storeName, 'collectionName' => $podName]);
        }

        return $this->transaction_collection->count(['status' => 'completed']);
    }

    // PROTECTED Functions

    /**
     * Proxy method to help with test mocking.
     *
     * @codeCoverageIgnore
     */
    protected function insertTransaction(array $transaction): InsertOneResult
    {
        return $this->transaction_collection->insertOne($transaction, ['w' => 1]);
    }

    /**
     * Proxy method to help with test mocking.
     *
     * @codeCoverageIgnore
     */
    protected function updateTransaction(array $query, array $update, array $options): UpdateResult
    {
        return $this->transaction_collection->updateOne($query, $update, $options);
    }
}
