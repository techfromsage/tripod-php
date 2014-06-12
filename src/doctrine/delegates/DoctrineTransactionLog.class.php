<?php
use Doctrine\ORM\Tools\Setup;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Tools\SchemaTool;

require_once TRIPOD_DIR . 'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR . 'doctrine/entitites/TransactionLogEntries.class.php';
require_once TRIPOD_DIR . 'doctrine/entitites/TransactionLogEntry.class.php';
require_once TRIPOD_DIR . 'ITransactionLog.php';

class DoctrineTransactionLog implements ITransactionLog
{
    const TRANSACTION_LOG_ENTRY = 'TransactionLogEntry';
    private $entityManager = null;
    public function __construct()
    {
        $config = MongoTripodConfig::getInstance(); //todo: read config, from config
        // the connection configuration
        $this->entityManager = EntityManager::create(
            array(
                'driver'   => $config->getTransactionLogDriver(),
                'user'     => $config->getTransactionLogUser(),
                'password' => $config->getTransactionLogPassword(),
                'dbname'   => $config->getTransactionLogDatabase(),
            ),
            Setup::createAnnotationMetadataConfiguration(array(TRIPOD_DIR.'/doctrine/entities'), false)
        );

        try
        {
            $tool = new SchemaTool($this->getEntityManager());
            $tool->createSchema(array(
                $this->getEntityManager()->getClassMetadata(self::TRANSACTION_LOG_ENTRY)
            ));
        }
        catch (Exception $e)
        {
            // ignore
        }
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
        $transactionLogEntry->setChanges($changes);
        $transactionLogEntry->setStatus("in_progress");
        $transactionLogEntry->setStartTime(new DateTime("now"));
        $transactionLogEntry->setOriginalCBDs($originalCBDs);
        $transactionLogEntry->setSessionId(((session_id() != '') ? session_id() : ''));

        try
        {
            $this->getEntityManager()->persist($transactionLogEntry);
            $this->getEntityManager()->flush();
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
        $transactionLogEntry = $this->getEntityManager()->find(self::TRANSACTION_LOG_ENTRY,$transaction_id);

        $transactionLogEntry->setStatus("cancelling");
        if ($error!=null)
        {
            $transactionLogEntry->setError(array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString()));
        }

        $this->getEntityManager()->persist($transactionLogEntry);
        $this->getEntityManager()->flush();
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
        $transactionLogEntry = $this->getEntityManager()->find(self::TRANSACTION_LOG_ENTRY,$transaction_id);

        $transactionLogEntry->setStatus("failed");
        $transactionLogEntry->setFailedTime(new DateTime("now"));
        if ($error!=null)
        {
            $transactionLogEntry->setError(array('reason'=>$error->getMessage(), 'trace'=>$error->getTraceAsString()));
        }

        $this->getEntityManager()->persist($transactionLogEntry);
        $this->getEntityManager()->flush();
    }

    /**
     * Update the status of a transaction to completed, and adds an end time
     *
     * @param string $transaction_id - the id of the transaction you want to mark as completed
     * @param array $newCBDs CBDs that represent the after state for each modified entity
     */
    public function completeTransaction($transaction_id, $newCBDs)
    {
        /* @var $transactionLogEntry TransactionLogEntry */
        $transactionLogEntry = $this->getEntityManager()->find(self::TRANSACTION_LOG_ENTRY,$transaction_id);

        $transactionLogEntry->setStatus("completed");
        $transactionLogEntry->setEndTime(new DateTime("now"));
        $transactionLogEntry->setNewCBDs($newCBDs);

        $this->getEntityManager()->persist($transactionLogEntry);
        $this->getEntityManager()->flush();
    }

    /**
     * Retrieves a transaction from the transaction based on its id.  The transaction is returned as an array
     *
     * @param $transaction_id - the id of the transaction you wish to retrieve from the transaction log
     * @return array representing the transaction document
     */
    public function getTransaction($transaction_id)
    {
        /* @var $transactionLogEntry TransactionLogEntry */
        $transactionLogEntry = $this->getEntityManager()->find(self::TRANSACTION_LOG_ENTRY,$transaction_id);
        return $transactionLogEntry->toArray();
    }

    /**
     * Purges all transactions from the transaction log
     */
    public function purgeAllTransactions()
    {
        $classes = array(
            $this->getEntityManager()->getClassMetadata(self::TRANSACTION_LOG_ENTRY)
        );
        $tool = new SchemaTool($this->getEntityManager());
        $tool->dropSchema($classes);
        $tool->createSchema($classes);
    }

    /**
     * @param string $dbName
     * @param string $collectionName
     * @param string|null $fromDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate only transactions after this specified date will be replayed. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @return Iterator
     * @throws InvalidArgumentException
     */
    public function getCompletedTransactions($dbName=null, $collectionName=null, $fromDate=null, $toDate=null)
    {
        $params = array("status"=>"completed");
        $qb = $this->getEntityManager()->createQueryBuilder()
            ->select('tlog')
            ->from(self::TRANSACTION_LOG_ENTRY,'tlog');

        $whereExpr = array($qb->expr()->eq("tlog.status",":status"));

        if(!empty($dbName))
        {
            $params['dbName'] = $dbName;
            $whereExpr[] =  $qb->expr()->eq('tlog.dbName',':dbName');
        }
        if (!empty($collectionName))
        {
            $params["collectionName"] = $collectionName;
            $whereExpr[] =  $qb->expr()->eq('tlog.collectionName',':collectionName');
        }

        if(!empty($fromDate)) {
            $params['fromDate'] = $fromDate;
            $whereExpr[] =  $qb->expr()->gte('tlog.endTime',':fromDate');
            if(!empty($toDate)){
                $params['toDate'] = $toDate;
                $whereExpr[] =  $qb->expr()->lte('tlog.endTime',':toDate');
            }
        }

        if (count($whereExpr)>1)
        {
            $qb->where(call_user_func_array(array($qb->expr(),'andX'), $whereExpr));
        }
        else
        {
            $qb->where($whereExpr);
        }
        $qb->setParameters($params);

        return new TransactionLogEntries($qb->getQuery()->iterate());
    }

    /**
     * @return int Total number of transactions in the transaction log
     */
    public function getTotalTransactionCount()
    {
        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->select('count(tlog.id)');
        $qb->from(self::TRANSACTION_LOG_ENTRY,'tlog');
        return $qb->getQuery()->getSingleScalarResult();
    }

    /**
     * @param dbName = database name to filter on (optional)
     * @param collectionName = collectionName to filter on (optional)
     * @return int Total number of completed transactions in the transaction log
     * @codeCoverageIgnore
     */
    public function getCompletedTransactionCount($dbName=null, $collectionName=null)
    {
        $qb = $this->getEntityManager()->createQueryBuilder()
            ->select('count(tlog.id)')
            ->from(self::TRANSACTION_LOG_ENTRY,'tlog')
            ->where("tlog.status = :status")
            ->setParameter('status','completed');
        return $qb->getQuery()->getSingleScalarResult();
    }

    /**
     * For mocking and data manipulation in tests
     * @return Doctrine\ORM\EntityManager|null
     */
    public function getEntityManager()
    {
        return $this->entityManager;
    }
}
