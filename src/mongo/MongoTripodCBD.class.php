<?php

class MongoTripodCBD implements iTripodGraph, iTripodTable {
    protected $config;
    protected $collection;
    public function __construct($name)
    {
        $this->config = MongoTripodConfig::getInstance();
        $this->collection = $this->config->getCollectionForCBD($name);
    }
    public function describe($query)
    {
        $this->collection->find();
    }

    public function select(array $query, array $fields, $context=null)
    {

    }
}