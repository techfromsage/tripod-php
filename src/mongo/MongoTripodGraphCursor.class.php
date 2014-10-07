<?php

class MongoTripodGraphCursor implements Iterator {

    protected $cursor;
    public function __construct(MongoCursor $cursor)
    {
        $this->cursor = $cursor;
    }

    public function current()
    {
        return $this->cursor->current();
    }

    public function key()
    {
        return $this->cursor->key();
    }

    public function next()
    {
        $this->cursor->next();
    }

    public function rewind()
    {
        $this->cursor->rewind();
    }

    public function valid()
    {
        $this->cursor->valid();
    }

    public function __call($method, $arguments)
    {
        if(method_exists($this->cursor, $method))
        {
            return $this->cursor->$method($arguments);
        }
    }
}