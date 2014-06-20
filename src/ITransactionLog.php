<?php
interface ITransactionLog
{
    /**
     * @param string $transaction_id - the id you wish to assign to the new transaction
     * @param array $changes - an array serialization of the changeset to be applied
     * @param array $originalCBDs - an array of the serialized CBDs
     * @param string $dbName - the name of the database the changes are being applied to
     * @param string $collectionName - the name of the collection, in the database, the changes are being applied to
     * @throws TripodException
     */
    public function createNewTransaction($transaction_id, $changes, $originalCBDs, $dbName, $collectionName);

    /**
     * Updates the status of a transaction to cancelling.
     * If you passed in an Exception, the exception is logged in the transaction log.
     *
     * @param $transaction_id - the id of the transaction you wish to cancel
     * @param Exception - pass in the exception you wish to log
     */
    public function cancelTransaction($transaction_id, Exception $error=null);

    /**
     * Updates the status of a transaction to failed, and adds a fail time.
     * If you passed in an Exception, the exception is logged in the transaction log
     *
     * @param $transaction_id - the id of the transaction you wish to set as failed
     * @param Exception - exception you wish to log
     */
    public function failTransaction($transaction_id, Exception $error=null);

    /**
     * Update the status of a transaction to completed, and adds an end time
     *
     * @param string $transaction_id - the id of the transaction you want to mark as completed
     * @param array $newCBDs array of CBD's that represent the after state for each modified entity
     */
    public function completeTransaction($transaction_id, $newCBDs);

    /**
     * Retrieves a transaction from the transaction based on its id.  The transaction is returned as an array
     *
     * @param $transaction_id - the id of the transaction you wish to retrieve from the transaction log
     * @return Array representing the transaction document
     */
    public function getTransaction($transaction_id);

    /**
     * Purges all transactions from the transaction log
     */
    public function purgeAllTransactions();

    /**
     * @param string $dbName
     * @param string $collectionName
     * @param string|null $fromDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @return Iterator
     * @throws InvalidArgumentException
     */
    public function getCompletedTransactions($dbName=null, $collectionName=null, $fromDate=null, $toDate=null);

    /**
     * @return int Total number of transactions in the transaction log
     */
    public function getTotalTransactionCount();

    /**
     * @param dbName = database name to filter on (optional)
     * @param collectionName = collectionName to filter on (optional)
     * @return int Total number of completed transactions in the transaction log
     * @codeCoverageIgnore
     */
    public function getCompletedTransactionCount($dbName=null, $collectionName=null);
}
