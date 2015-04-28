<?php

/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation
 */
class ModifiedSubject implements SplSubject
{
    /* @var $observer IComposite */
    private $observer;
    private $data;

    public function __construct($data,IComposite $composite)
    {
        $this->data = $data;
        $this->attach($composite);
    }

    public function getData()
    {
        return $this->data;
    }

    public function getOperation()
    {
        return $this->observer->getOperationType();
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Attach an SplObserver
     * @link http://php.net/manual/en/splsubject.attach.php
     * @param SplObserver $observer <p>
     * The <b>SplObserver</b> to attach.
     * </p>
     * @return void
     */
    public function attach(SplObserver $observer)
    {
        $this->observer = $observer;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Detach an observer
     * @link http://php.net/manual/en/splsubject.detach.php
     * @param SplObserver $observer <p>
     * The <b>SplObserver</b> to detach.
     * </p>
     * @return void
     */
    public function detach(SplObserver $observer)
    {
        $this->observer = null;
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Notify an observer
     * @link http://php.net/manual/en/splsubject.notify.php
     * @return void
     */
    public function notify()
    {
        /* @var $observer SplObserver */
        $this->observer->update($this);
    }

    /**
     * Creates a modified subject based on the supplied data
     * @param array $resourceId
     * @param array $types
     * @param array $operations
     * @param array $specTypes
     * @param string $storeName
     * @param string $podName
     * @param bool $delete
     * @return ModifiedSubject
     * @throws TripodException
     */
    public static function create(Array $resourceId, SplObserver $observer, Array $specTypes, $storeName, $podName, $delete=false)
    {
        if (!is_array($resourceId) || !array_key_exists(_ID_RESOURCE,$resourceId) || !array_key_exists(_ID_CONTEXT,$resourceId))
        {
            throw new TripodException('Parameter $resourceId needs to be of type array with ' . _ID_RESOURCE . ' and ' . _ID_CONTEXT . ' keys');
        }

        $data = array(
            _ID_RESOURCE=>$resourceId[_ID_RESOURCE],
            _ID_CONTEXT=>$resourceId[_ID_CONTEXT],
            "database"=>$storeName,
            "collection"=>$podName,
        );
        if(isset($resourceId[_ID_TYPE]))
        {
            $data[_ID_TYPE] = $resourceId[_ID_TYPE];
        }

        if(!empty($specTypes))
        {
            $data['specTypes'] = $specTypes;
        }

        if($delete==true)
        {
            $data['delete']=true;
        }

        return new ModifiedSubject($data,$observer);
    }
}