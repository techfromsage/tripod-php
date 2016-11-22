<?php

namespace Tripod\Mongo;
require_once TRIPOD_DIR . 'mongo/Config.class.php';

use \MongoDB\BSON\UTCDateTime;
use \MongoDB\InsertOneResult;
use \MongoDB\UpdateOneResult;
use \MongoDB\Driver\Cursor;

/**
 * Class TransactionLog
 * @package Tripod\Mongo
 */
class TransactionLog
{
    private $transaction_db = null;
    private $transaction_collection = null;
    protected $config = null;

    /**
     * Constructor
     */
    public function __construct()
    {
        $config = Config::getInstance();
        $this->config = $config->getTransactionLogConfig();
        $this->transaction_db = $config->getTransactionLogDatabase();
        $this->transaction_collection = $this->transaction_db->selectCollection($this->config['collection']);
    }

    /**
     * @param string $transaction_id - the id you wish to assign to the new transaction
     * @param array $changes - an array serialization of the changeset to be applied
     * @param array $originalCBDs - an array of the serialized CBDs
     * @param string $storeName - the name of the database the changes are being applied to
     * @param string $podName - the name of the collection, in the database, the changes are being applied to
     * @throws \Tripod\Exceptions\Exception
     */
    public function createNewTransaction($transaction_id, $changes, $originalCBDs, $storeName, $podName)
    {
        $transaction = array(
            "_id" => $transaction_id,
            "dbName"=>$storeName,
            "collectionName"=>$podName,
            "changes" => $changes,
            "status" => "in_progress",
            "startTime" => new UTCDateTime(floor(microtime(true) * 1000)),
            "originalCBDs"=>$originalCBDs,
            "sessionId" => ((session_id() != '') ? session_id() : '')
        );

        try {
            $result = $this->insertTransaction($transaction);
            if (!$result->isAcknowledged()) {
                throw new \Exception('Error creating new transaction');
            }
        } catch(\Exception $e) {
            throw new \Tripod\Exceptions\Exception("Error creating new transaction: " . $e->getMessage());
        }
    }

    /**
     * Updates the status of a transaction to cancelling.
     * If you passed in an Exception, the exception is logged in the transaction log.
     *
     * @param string $transaction_id the id of the transaction you wish to cancel
     * @param \Exception $error pass in the exception you wish to log
     */
    public function cancelTransaction($transaction_id, \Exception $error=null)
    {
        $params = array('status' => 'cancelling');
        if($error!=null)
        {
            $params['error'] = array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString());
        }

        $this->updateTransaction(
            array("_id" => $transaction_id),
            array('$set' => $params),
            array("w" => 1, 'upsert'=>true)
        );
    }

    /**
     * Updates the status of a transaction to failed, and adds a fail time.
     * If you passed in an Exception, the exception is logged in the transaction log
     *
     * @param string $transaction_id the id of the transaction you wish to set as failed
     * @param \Exception $error exception you wish to log
     */
    public function failTransaction($transaction_id, \Exception $error=null)
    {
        $params = array('status' => 'failed', 'failedTime' => new UTCDateTime(floor(microtime(true) * 1000)));
        if($error!=null)
        {
            $params['error'] = array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString());
        }

        $this->updateTransaction(
            array("_id" => $transaction_id),
            array('$set' => $params),
            array('w' => 1, 'upsert'=>true)
        );
    }

    /**
     * Update the status of a transaction to completed, and adds an end time
     *
     * @param string $transaction_id - the id of the transaction you want to mark as completed
     * @param array $newCBDs array of CBD's that represent the after state for each modified entity
     */
    public function completeTransaction($transaction_id, Array $newCBDs)
    {

        $this->updateTransaction(
            array("_id" => $transaction_id),
            array('$set' => array('status' => 'completed', 'endTime' => new UTCDateTime(floor(microtime(true) * 1000)), 'newCBDs'=>$newCBDs)),
            array('w' => 1)
        );
    }

    /**
     * Retrieves a transaction from the transaction based on its id.  The transaction is returned as an array
     *
     * @param string $transaction_id - the id of the transaction you wish to retrieve from the transaction log
     * @return Array representing the transaction document
     */
    public function getTransaction($transaction_id)
    {
        return $this->transaction_collection->findOne(array("_id"=>$transaction_id));

    }

    /**
     * Purges all transactions from the transaction log
     */
    public function purgeAllTransactions()
    {
        $this->transaction_collection->drop();
    }

    /**
     * @param string $storeName
     * @param string $podName
     * @param string|null $fromDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @return Cursor
     * @throws \InvalidArgumentException
     */
    public function getCompletedTransactions($storeName=null, $podName=null, $fromDate=null, $toDate=null)
    {
        $query = array();
        $query['status'] = 'completed';

        if(!empty($storeName) && !empty($podName))
        {
            $query['dbName'] = $storeName;
            $query['collectionName'] = $podName;
        }

        if(!empty($fromDate)) {
            $q = array();
            $q['$gte'] = new UTCDateTime(strtotime($fromDate)*1000);

            if(!empty($toDate)){
                $q['$lte'] = new UTCDateTime(strtotime($toDate)*1000);
            }

            $query['endTime'] = $q;
        }

        return $this->transaction_collection->find($query, array('sort' => array('endTime'=>1)));
    }

    /**
     * @return int Total number of transactions in the transaction log
     */
    public function getTotalTransactionCount()
    {
        return $this->transaction_collection->count(array());
    }

    /**
     * @param string $storeName database name to filter on (optional)
     * @param string $podName collectionName to filter on (optional)
     * @return int Total number of completed transactions in the transaction log
     * @codeCoverageIgnore
     */
    public function getCompletedTransactionCount($storeName=null, $podName=null)
    {
        if(!empty($storeName) && !empty($podName))
        {
            return $this->transaction_collection->count(array('status'=>'completed','dbName'=>$storeName, 'collectionName'=>$podName));
        }
        else
        {
            return $this->transaction_collection->count(array('status'=>'completed'));
        }
    }

    /* PROTECTED Functions */

    /**
     * Proxy method to help with test mocking
     * @param array $transaction
     * @return InsertOneResult
     * @codeCoverageIgnore
     */
    protected function insertTransaction($transaction)
    {
        return $this->transaction_collection->insertOne($transaction, array("w" => 1));
    }

    /**
     * Proxy method to help with test mocking
     * @param array $query
     * @param array $update
     * @param array $options
     * @return UpdateOneResult
     * @codeCoverageIgnore
     */
    protected function updateTransaction($query, $update, $options)
    {
        return $this->transaction_collection->updateOne($query, $update, $options);
    }


}
