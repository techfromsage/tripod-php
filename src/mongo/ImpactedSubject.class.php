<?php

namespace Tripod\Mongo;
/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation
 */
class ImpactedSubject
{
    /**
     * @var string
     */
    private $operation;

    /**
     * @var array
     */
    private $resourceId;

    /**
     * @var array
     */
    private $specTypes;

    /**
     * @var string
     */
    private $storeName;

    /**
     * @var string
     */
    private $podName;

    /**
     * @param array $resourceId
     * @param string $operation
     * @param string $storeName
     * @param string $podName
     * @param array $specTypes
     * @throws \Tripod\Exceptions\Exception
     * @throws \Exception
     */
    public function __construct(Array $resourceId, $operation, $storeName, $podName, Array $specTypes=array())
    {
        if (!is_array($resourceId) || !array_key_exists(_ID_RESOURCE,$resourceId) || !array_key_exists(_ID_CONTEXT,$resourceId))
        {
            throw new \Tripod\Exceptions\Exception('Parameter $resourceId needs to be of type array with ' . _ID_RESOURCE . ' and ' . _ID_CONTEXT . ' keys');
        }
        else
        {
            $this->resourceId = $resourceId;
        }

        if (in_array($operation,array(OP_VIEWS,OP_TABLES,OP_SEARCH)))
        {
            $this->operation = $operation;
        }
        else
        {
            throw new \Exception("Invalid operation: $operation");
        }

        $this->storeName = $storeName;
        $this->podName = $podName;
        $this->specTypes = $specTypes;
    }

    /**
     * @return string
     */
    public function getOperation()
    {
        return $this->operation;
    }

    /**
     * @return string
     */
    public function getPodName()
    {
        return $this->podName;
    }

    /**
     * @return array
     */
    public function getResourceId()
    {
        return $this->resourceId;
    }

    /**
     * @return array
     */
    public function getSpecTypes()
    {
        return $this->specTypes;
    }

    /**
     * @return string
     */
    public function getStoreName()
    {
        return $this->storeName;
    }

    /**
     * Serialises the data as an array
     * @return array
     */
    public function toArray()
    {
        return array(
            "resourceId" => $this->resourceId,
            "operation" => $this->operation,
            "specTypes" => $this->specTypes,
            "storeName" => $this->storeName,
            "podName" => $this->podName
        );
    }

    /**
     * Perform the update on the composite defined by the operation
     */
    public function update()
    {
        $tripod = $this->getTripod();
        $tripod->getComposite($this->operation)->update($this);
    }

    /**
     * For mocking
     * @return Tripod
     */
    protected function getTripod()
    {
        return new Tripod($this->getPodName(),$this->getStoreName(),array("readPreference"=>\MongoClient::RP_PRIMARY));
    }
}