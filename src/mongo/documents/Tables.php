<?php

declare(strict_types=1);

namespace Tripod\Mongo\Documents;

use MongoDB\Model\BSONDocument;

class Tables extends BSONDocument
{
    public function __construct(array $input = [])
    {
        parent::__construct($this->toTableRow($input));
    }

    /**
     * Sets the array value to the modeled table row value.
     *
     * @param array $data DB document array
     */
    public function bsonUnserialize(array $data): void
    {
        $this->exchangeArray($this->toTableRow($data));
    }

    /**
     * Models the table row from the source data.
     *
     * @param array $doc Database document
     *
     * @return array
     */
    protected function toTableRow(array $doc)
    {
        $result = $doc['value'] ?? [];
        if (isset($result[_IMPACT_INDEX])) {
            unset($result[_IMPACT_INDEX]);
        }

        return $result;
    }
}
