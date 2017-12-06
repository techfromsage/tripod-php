<?php

namespace Tripod\Mongo;

class JobGroup
{
    private $id;
    private $collection;
    private $storeName;

    public function __construct($storeName, $groupId = null)
    {
        $this->storeName = $storeName;
        if (!$groupId) {
            $groupId = new \MongoDB\BSON\ObjectId();
        } elseif (!$groupId instanceof \MongoDB\BSON\ObjectId) {
            $groupId = new \MongoDB\BSON\ObjectId($groupId);
        }
        $this->id = $groupId;
    }

    public function setJobCount($count)
    {
        $this->getMongoCollection()->updateOne(
            ['_id' => $this->id],
            ['$set' => ['count' => $count]],
            ['upsert' => true]
        );
    }

    public function incrementJobCount($inc = 1)
    {
        $updateResult = $this->getMongoCollection()->findOneAndUpdate(
            ['_id' => $this->id],
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
     * @return \MongoDB\BSON\ObjectId
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
