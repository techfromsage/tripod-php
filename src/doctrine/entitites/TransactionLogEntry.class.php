<?php
/**
 * @Entity @Table(name="transaction_log")
 **/
class TransactionLogEntry
{
    /** @Id @Column(type="integer") **/
    protected $id;

    /** @Column(type="string") **/
    protected $dbName;

    /** @Column(type="string") **/
    protected $collectionName;

    /** @Column(type="string") **/
    protected $changes;

    /** @Column(type="string") **/
    protected $status;

    /** @Column(type="datetime") **/
    protected $startTime;

    /** @Column(type="string") **/
    protected $originalCBDs;

    /** @Column(type="string") **/
    protected $sessionId;

    public function setChanges($changes)
    {
        $this->changes = $changes;
    }

    public function getChanges()
    {
        return $this->changes;
    }

    public function setCollectionName($collectionName)
    {
        $this->collectionName = $collectionName;
    }

    public function getCollectionName()
    {
        return $this->collectionName;
    }

    public function setDbName($dbName)
    {
        $this->dbName = $dbName;
    }

    public function getDbName()
    {
        return $this->dbName;
    }

    public function setId($id)
    {
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setOriginalCBDs($originalCBDs)
    {
        $this->originalCBDs = $originalCBDs;
    }

    public function getOriginalCBDs()
    {
        return $this->originalCBDs;
    }

    public function setSessionId($sessionId)
    {
        $this->sessionId = $sessionId;
    }

    public function getSessionId()
    {
        return $this->sessionId;
    }

    public function setStartTime($startTime)
    {
        $this->startTime = $startTime;
    }

    public function getStartTime()
    {
        return $this->startTime;
    }

    public function setStatus($status)
    {
        $this->status = $status;
    }

    public function getStatus()
    {
        return $this->status;
    }


}