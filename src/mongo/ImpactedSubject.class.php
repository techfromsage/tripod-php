<?php

/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation
 * todo: this is misnamed. Instead it should be ImpactedSubject. Remove SplSubject interface as no longer makes sense with 1-1 mapping to observer.
 */
class ImpactedSubject
{
    /**
     * @var String
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
     * @var bool
     */
    private $delete;

    public function __construct(Array $resourceId, $operation, $storeName, $podName, Array $specTypes=array(), $delete=false )
    {
        if (!is_array($resourceId) || !array_key_exists(_ID_RESOURCE,$resourceId) || !array_key_exists(_ID_CONTEXT,$resourceId))
        {
            throw new TripodException('Parameter $resourceId needs to be of type array with ' . _ID_RESOURCE . ' and ' . _ID_CONTEXT . ' keys');
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
            throw new Exception("Invalid operation: $operation");
        }

        $this->storeName = $storeName;
        $this->podName = $podName;
        $this->specTypes = $specTypes;
        $this->delete = $delete;

    }

    /**
     * @return boolean
     */
    public function getDelete()
    {
        return $this->delete;
    }

    /**
     * @return String
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
            "podName" => $this->podName,
            "delete" => $this->delete
        );
    }

    public function update()
    {
        $tripod = new MongoTripod($this->getPodName(),$this->getStoreName(),array("readPreference"=>MongoClient::RP_PRIMARY));
        $tripod->getComposite($this->operation)->update($this);
    }
}