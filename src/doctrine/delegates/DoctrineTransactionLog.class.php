<?php
use Doctrine\ORM\Tools\Setup;
use Doctrine\ORM\EntityManager;

require_once TRIPOD_DIR . 'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR . 'doctrine/entitites/TransactionLogEntry.class.php';
require_once TRIPOD_DIR . 'ITransactionLog.php';

class PostgresTransactionLog implements ITransactionLog
{
    private $entityManager = null;
    public function __construct()
    {
        $config = MongoTripodConfig::getInstance(); //todo: read config, from config
        // the connection configuration
        $this->entityManager = EntityManager::create(
            array(
                'driver'   => 'pdo_pgsql',
                'user'     => 'root',
                'password' => '',
                'dbname'   => 'tlog',
            ),
            Setup::createAnnotationMetadataConfiguration(array(TRIPOD_DIR.'/doctrine/entities'), false)
        );
    }

    /**
     * @param string $transaction_id - the id you wish to assign to the new transaction
     * @param array $changes - an array serialization of the changeset to be applied
     * @param array $originalCBDs - an array of the serialized CBDs
     * @param string $dbName - the name of the database the changes are being applied to
     * @param string $collectionName - the name of the collection, in the database, the changes are being applied to
     * @throws TripodException
     */
    public function createNewTransaction($transaction_id, $changes, $originalCBDs, $dbName, $collectionName)
    {
        $transactionLogEntry = new TransactionLogEntry();
        $transactionLogEntry->setId($transaction_id);
        $transactionLogEntry->setDbName($dbName);
        $transactionLogEntry->setCollectionName($collectionName);
        $transactionLogEntry->setChanges(json_encode($changes));
        $transactionLogEntry->setStatus("in_progress");
        $transactionLogEntry->setStartTime(new DateTime("now"));
        $transactionLogEntry->setOriginalCBDs(json_encode($originalCBDs));
        $transactionLogEntry->setSessionId(((session_id() != '') ? session_id() : ''));

        try
        {
            $this->entityManager->persist($transactionLogEntry);
            $this->entityManager->flush();
        }
        catch (Exception $e)
        {
            throw new TripodException("Error creating new transaction: " . $e->getMessage());
        }
    }

    /**
     * Updates the status of a transaction to cancelling.
     * If you passed in an Exception, the exception is logged in the transaction log.
     *
     * @param $transaction_id - the id of the transaction you wish to cancel
     * @param Exception - pass in the exception you wish to log
     */
    public function cancelTransaction($transaction_id, Exception $error=null)
    {
        /* @var $transactionLogEntry TransactionLogEntry */
        $transactionLogEntry = $this->entityManager->find("TransactionLogEntry",$transaction_id);

        $transactionLogEntry->setStatus("cancelling");
        if ($error!=null)
        {
            $transactionLogEntry->setError(json_encode(array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString())));
        }

        $this->entityManager->persist($transactionLogEntry);
        $this->entityManager->flush();
//        $params = array('status' => 'cancelling');
//        if($error!=null)
//        {
//            $params['error'] =
//        }
//
//        $this->updateTransaction(
//            array("_id" => $transaction_id),
//            array('$set' => $params),
//            array("w" => 1, 'upsert'=>true)
//        );
    }

    /**
     * Updates the status of a transaction to failed, and adds a fail time.
     * If you passed in an Exception, the exception is logged in the transaction log
     *
     * @param $transaction_id - the id of the transaction you wish to set as failed
     * @param Exception - exception you wish to log
     */
    public function failTransaction($transaction_id, Exception $error=null)
    {
        /* @var $transactionLogEntry TransactionLogEntry */
        $transactionLogEntry = $this->entityManager->find("TransactionLogEntry",$transaction_id);

        $transactionLogEntry->setStatus("failed");
        $transactionLogEntry->setFailedTime(new DateTime("now"));
        if ($error!=null)
        {
            $transactionLogEntry->setError(json_encode(array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString())));
        }

        $this->entityManager->persist($transactionLogEntry);
        $this->entityManager->flush();
//        $params = array('status' => 'failed', 'failedTime' => new MongoDate());
//        if($error!=null)
//        {
//            $params['error'] = array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString());
//        }
//
//        $this->updateTransaction(
//            array("_id" => $transaction_id),
//            array('$set' => $params),
//            array('w' => 1, 'upsert'=>true)
//        );
    }

    /**
     * Update the status of a transaction to completed, and adds an end time
     *
     * @param $transaction_id - the id of the transaction you want to mark as completed
     * @param $newCBDs array of CBD's that represent the after state for each modified entity
     */
    public function completeTransaction($transaction_id, $newCBDs)
    {
        /* @var $transactionLogEntry TransactionLogEntry */
        $transactionLogEntry = $this->entityManager->find("TransactionLogEntry",$transaction_id);

        $transactionLogEntry->setStatus("completed");
        $transactionLogEntry->setEndTime(new DateTime("now"));

        $this->entityManager->persist($transactionLogEntry);
        $this->entityManager->flush();
//        $this->updateTransaction(
//            array("_id" => $transaction_id),
//            array('$set' => array('status' => 'completed', 'endTime' => new MongoDate(), 'newCBDs'=>$newCBDs)),
//            array('w' => 1)
//        );
    }

    /**
     * Retrieves a transaction from the transaction based on its id.  The transaction is returned as an array
     *
     * @param $transaction_id - the id of the transaction you wish to retrieve from the transaction log
     * @return Array representing the transaction document
     */
    public function getTransaction($transaction_id)
    {
        // todo
//        return $this->transaction_collection->findOne(array("_id"=>$transaction_id));
    }

    /**
     * Purges all transactions from the transaction log
     */
    public function purgeAllTransactions()
    {
        // todo: more efficient way of doing this??
        $repository = $this->entityManager->getRepository('TransactionLogEntry');
        foreach($repository->findAll() as $entry) {
            $this->entityManager->remove($entry);
        }
        $this->entityManager->flush();
//        $this->transaction_collection->drop();
    }

    /**
     * @param string $dbName
     * @param string $collectionName
     * @param string|null $fromDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @return MongoCursor
     * @throws InvalidArgumentException
     */
    public function getCompletedTransactions($dbName=null, $collectionName=null, $fromDate=null, $toDate=null)
    {
        // todo
//        $query = array();
//        $query['status'] = 'completed';
//
//        if(!empty($dbName) && !empty($collectionName))
//        {
//            $query['dbName'] = $dbName;
//            $query['collectionName'] = $collectionName;
//        }
//
//        if(!empty($fromDate)) {
//            $q = array();
//            $q['$gte'] = new MongoDate(strtotime($fromDate));
//
//            if(!empty($toDate)){
//                $q['$lte'] = new MongoDate(strtotime($toDate));
//            }
//
//            $query['endTime'] = $q;
//        }
//
//        return $this->transaction_collection->find($query)->sort(array('endTime'=>1));
    }

    /**
     * @return int Total number of transactions in the transaction log
     */
    public function getTotalTransactionCount()
    {
        // todo
//        return $this->transaction_collection->count(array());
    }

    /**
     * @param dbName = database name to filter on (optional)
     * @param collectionName = collectionName to filter on (optional)
     * @return int Total number of completed transactions in the transaction log
     * @codeCoverageIgnore
     */
    public function getCompletedTransactionCount($dbName=null, $collectionName=null)
    {
        // todo
//        if(!empty($dbName) && !empty($collectionName))
//        {
//            return $this->transaction_collection->count(array('status'=>'completed','dbName'=>$dbName, 'collectionName'=>$collectionName));
//        }
//        else
//        {
//            return $this->transaction_collection->count(array('status'=>'completed'));
//        }
    }
}
