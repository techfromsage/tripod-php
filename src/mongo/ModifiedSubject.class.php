<?php

/**
 * A subject that has been involved in an modification event (create/update, delete) and will therefore require
 * view, table and search doc generation
 */
class ModifiedSubject implements SplSubject
{
    private $observers;
    private $data;

    public function __construct($data)
    {
        $this->data = $data;
        $this->observers = new SplObjectStorage();
    }

    public function getData()
    {
        return $this->data;
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
        $this->observers->attach($observer);
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
        $this->observers->detach($observer);
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Notify an observer
     * @link http://php.net/manual/en/splsubject.notify.php
     * @return void
     */
    public function notify()
    {
        foreach ($this->observers as $observer)
        {
            /* @var $observer SplObserver */
            $observer->update($this);
        }
    }

    /**
     * Creates a modified subject based on the supplied data
     * @param array $resourceId
     * @param array $types
     * @param array $operations
     * @param $dbName
     * @param $collectionName
     * @param bool $delete
     * @return ModifiedSubject
     * @throws TripodException
     */
    public static function create(Array $resourceId, Array $types, Array $operations, $dbName, $collectionName, $delete=false)
    {
        if (!is_array($resourceId) || !array_key_exists("r",$resourceId) || !array_key_exists("c",$resourceId))
        {
            throw new TripodException('Parameter $resourceId needs to be of type array with r and c keys');
        }

        $data = array(
            _ID_RESOURCE=>$resourceId[_ID_RESOURCE],
            _ID_CONTEXT=>$resourceId[_ID_CONTEXT],
            "database"=>$dbName,
            "collection"=>$collectionName,
            "operations"=>$operations
        );
        if(isset($resourceId[_ID_TYPE]))
        {
            $data[_ID_TYPE] = $resourceId[_ID_TYPE];
        }

        if(!empty($types))
        {
            $data['rdf:type'] = $types;
        }

        if($delete==true)
        {
            $data['delete']=true;
        }

        return new ModifiedSubject($data);
    }
}