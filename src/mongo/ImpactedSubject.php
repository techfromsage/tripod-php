<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Driver\ReadPreference;
use Tripod\Exceptions\Exception;
use Tripod\ITripodStat;

/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation.
 *
 * @phpstan-type ResourceId array{r: string, c: string}
 */
class ImpactedSubject
{
    private string $operation;

    /**
     * @var ResourceId
     */
    private array $resourceId;

    private array $specTypes;

    private string $storeName;

    private string $podName;

    private ?ITripodStat $tripodStat = null;

    /**
     * @throws Exception
     */
    public function __construct(array $resourceId, string $operation, string $storeName, string $podName, array $specTypes = [], ?ITripodStat $stat = null)
    {
        if (!isset($resourceId[_ID_RESOURCE], $resourceId[_ID_CONTEXT])
            || !is_string($resourceId[_ID_RESOURCE]) || !is_string($resourceId[_ID_CONTEXT])
        ) {
            throw new Exception('Parameter $resourceId needs to be of type array with ' . _ID_RESOURCE . ' and ' . _ID_CONTEXT . ' keys');
        }

        $this->resourceId = [_ID_RESOURCE => $resourceId[_ID_RESOURCE], _ID_CONTEXT => $resourceId[_ID_CONTEXT]];

        if (in_array($operation, [OP_VIEWS, OP_TABLES, OP_SEARCH])) {
            $this->operation = $operation;
        } else {
            throw new Exception('Invalid operation: ' . $operation);
        }

        $this->storeName = $storeName;
        $this->podName = $podName;
        $this->specTypes = $specTypes;

        if ($stat instanceof ITripodStat) {
            $this->tripodStat = $stat;
        }
    }

    public function getOperation(): string
    {
        return $this->operation;
    }

    public function getPodName(): string
    {
        return $this->podName;
    }

    public function getResourceId(): array
    {
        return $this->resourceId;
    }

    public function getSpecTypes(): array
    {
        return $this->specTypes;
    }

    public function getStoreName(): string
    {
        return $this->storeName;
    }

    /**
     * Serialises the data as an array.
     */
    public function toArray(): array
    {
        return [
            'resourceId' => $this->resourceId,
            'operation' => $this->operation,
            'specTypes' => $this->specTypes,
            'storeName' => $this->storeName,
            'podName' => $this->podName,
        ];
    }

    /**
     * Perform the update on the composite defined by the operation.
     */
    public function update(): void
    {
        $tripod = $this->getTripod();
        if ($this->tripodStat !== null) {
            $tripod->setStat($this->tripodStat);
        }

        $tripod->getComposite($this->operation)->update($this);
    }

    /**
     * For mocking.
     */
    protected function getTripod(): Driver
    {
        return new Driver($this->getPodName(), $this->getStoreName(), [
            'readPreference' => ReadPreference::PRIMARY,
        ]);
    }
}
