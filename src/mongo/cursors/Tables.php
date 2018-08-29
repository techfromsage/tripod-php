<?php

namespace Tripod\Mongo\Cursors;

class Tables extends \MongoDB\Driver\Cursor
{
    public function current()
    {
        return $this->toTableRow(parent::current());
    }

    public function next()
    {
        return $this->toTableRow(parent::next());
    }

    protected function toTableRow(array $doc)
    {
        $result = isset($doc['value']) ? $doc['value'] : [];
        if (isset($result['value'][_IMPACT_INDEX])) {
            unset($result['value'][_IMPACT_INDEX]);
        }
        return $result;
    }
}
