<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\BSON\ObjectId;
use MongoDB\Collection;
use MongoDB\Operation\FindOneAndUpdate;
use Tripod\Config;

class JobGroup
{
    private ObjectId $id;

    private ?Collection $collection = null;

    private string $storeName;

    /**
     * Constructor method.
     *
     * @param string          $storeName Tripod store (database) name
     * @param ObjectId|string $groupId   Optional tracking ID, will assign a new one if omitted
     */
    public function __construct(string $storeName, $groupId = null)
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
     * Update the number of jobs.
     *
     * @param int $count Number of jobs in group
     */
    public function setJobCount(int $count): void
    {
        $this->getMongoCollection()->updateOne(
            [_ID_KEY => $this->getId()],
            ['$set' => ['count' => $count]],
            ['upsert' => true]
        );
    }

    /**
     * Update the number of jobs by $inc.  To decrement, use a negative integer.
     *
     * @param int $inc Number to increment or decrement by
     *
     * @return int Updated job count
     */
    public function incrementJobCount(int $inc = 1): int
    {
        $updateResult = $this->getMongoCollection()->findOneAndUpdate(
            [_ID_KEY => $this->getId()],
            ['$inc' => ['count' => $inc]],
            ['upsert' => true, 'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
        );
        if (\is_array($updateResult)) {
            return $updateResult['count'];
        }

        // With upsert: true a document is always returned as an array (Tripod's client typeMap)
        throw new \RuntimeException('Failed to update job count for job group ' . $this->getId());
    }

    public function getId(): ObjectId
    {
        return $this->id;
    }

    /**
     * For mocking.
     */
    protected function getMongoCollection(): Collection
    {
        if ($this->collection === null) {
            $config = Config::getInstance();

            $this->collection = $config->getCollectionForJobGroups($this->storeName);
        }

        return $this->collection;
    }
}
