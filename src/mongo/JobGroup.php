<?php

namespace Tripod\Mongo;

use \MongoDB\BSON\ObjectId;

class JobGroup
{
    private $id;
    private $collection;
    private $storeName;

    /**
     * Constructor method
     * @param string                        $storeName Tripod store (database) name
     * @param string|ObjectId $groupId   Optional tracking ID, will assign a new one if omitted
     */
    public function __construct($storeName, $groupId = null)
    {
        $this->storeName = $storeName;
        if (!$groupId) {
            $groupId = new ObjectId();
        } elseif (!$groupId instanceof ObjectId) {
            $groupId = new ObjectId($groupId);
        }
        $this->id = $groupId;
    }

    /**
     * Update the number of jobs
     *
     * @param integer $count Number of jobs in group
     * @return void
     */
    public function setJobCount($count)
    {
        $this->getMongoCollection()->updateOne(
            ['_id' => $this->getId()],
            ['$set' => ['count' => $count]],
            ['upsert' => true]
        );
    }

    /**
     * Update the number of jobs by $inc.  To decrement, use a negative integer
     *
     * @param integer $inc Number to increment or decrement by
     * @return integer Updated job count
     */
    public function incrementJobCount($inc = 1)
    {
        $updateResult = $this->getMongoCollection()->findOneAndUpdate(
            ['_id' => $this->getId()],
            ['$inc' => ['count' => $inc]],
            ['upsert' => true, 'returnDocument' => \MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
        );
        if (\is_array($updateResult)) {
            return $updateResult['count'];
        } elseif (isset($updateResult->count)) {
            return $updateResult->count;
        }
    }

    /**
     * @return ObjectId
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * For mocking
     *
     * @return \MongoDB\Collection
     */
    protected function getMongoCollection()
    {
        if (!isset($this->collection)) {
            $config = Config::getInstance();

            $this->collection = $config->getCollectionForJobGroups($this->storeName);
        }
        return $this->collection;
    }
}
